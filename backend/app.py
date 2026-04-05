"""
Flask application for College Data Enrichment CRM.
Main entry point — API endpoints, file upload, processing, SSE, and static file serving.
"""

import os
import sys
import json
import time
import uuid
import threading
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from flask import Flask, request, jsonify, send_file, Response, send_from_directory
from werkzeug.utils import secure_filename
from flask_cors import CORS

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


from database import (
    init_db, create_job, update_job_status, update_job_progress,
    get_job, insert_colleges_batch, update_college_result,
    get_pending_colleges, get_colleges_by_job, get_college_count_by_status,
    add_log, get_logs, get_logs_since, get_job_summary
)
from scraper import (
    scrape_college_website, normalize_url, 
    domain_failures, domain_cooldown
)
from search_utils import find_college_website
from extractor import extract_all, classify_status

import warnings
warnings.filterwarnings('ignore', message='Unverified HTTPS request')
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─── App Configuration ───────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UPLOAD_DIR = os.path.join(BASE_DIR, 'uploads')
OUTPUT_DIR = os.path.join(BASE_DIR, 'outputs')
FRONTEND_DIR = os.path.join(BASE_DIR, 'frontend')

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

app = Flask(__name__, static_folder=FRONTEND_DIR, static_url_path='')
CORS(app)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB

ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}
REQUIRED_COLUMNS = ['College Name']

# ─── Job State Management ────────────────────────────────────────

# In-memory state for active jobs (thread-safe via GIL for simple ops)
active_jobs = {}  # job_id -> {'thread': Thread, 'paused': Event, 'cancelled': bool}
job_events = {}   # job_id -> list of SSE event dicts


def emit_event(job_id, event_type, data):
    """Push an SSE event to the job's event queue."""
    if job_id not in job_events:
        job_events[job_id] = []
    job_events[job_id].append({
        'type': event_type,
        'data': data,
        'timestamp': datetime.now().isoformat()
    })
    # Keep only last 500 events in memory
    if len(job_events[job_id]) > 500:
        job_events[job_id] = job_events[job_id][-500:]


# ─── File Helpers ─────────────────────────────────────────────────

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def read_upload(filepath):
    """Read uploaded file into a pandas DataFrame."""
    ext = filepath.rsplit('.', 1)[1].lower()
    if ext == 'csv':
        # Try multiple encodings
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                return pd.read_csv(filepath, encoding=encoding)
            except UnicodeDecodeError:
                continue
        raise ValueError("Could not decode CSV file")
    else:
        return pd.read_excel(filepath, engine='openpyxl')


def validate_columns(df):
    """Validate that required columns exist. Returns (ok, missing_cols)."""
    existing = [c.strip() for c in df.columns]
    df.columns = existing
    missing = [c for c in REQUIRED_COLUMNS if c not in existing]
    return len(missing) == 0, missing


# ─── Processing Engine ───────────────────────────────────────────

import asyncio
import aiohttp

async def process_single_college(college, session):
    college_name = college.get('college_name', '')
    state = college.get('state', '')
    district = college.get('district', '')
    website = college.get('original_website', '')
    search_method = 'direct'
    found_website = None
    errors = []

    existing_phone = college.get('extracted_phone', '')
    existing_email = college.get('extracted_email', '')
    if existing_phone and existing_email and existing_phone != 'Not Found' and existing_email != 'Not Found':
        return existing_phone, existing_email, college.get('extracted_principal', 'Not Found'), 'Active', 'skipped', website, None

    target_url = normalize_url(website) if website else None

    if not target_url:
        found_url, method = await asyncio.to_thread(find_college_website, college_name, state, district)
        if found_url:
            target_url = found_url
            found_website = found_url
            search_method = method
        else:
            return "Not Found", "Not Found", "Not Found", "Not Found", "none", None, "No website found via search"

    scrape_result = await scrape_college_website(target_url, session)

    if scrape_result['errors']:
        errors.extend(scrape_result['errors'])

    if scrape_result['texts']:
        extracted = await asyncio.to_thread(extract_all, scrape_result['texts'])
        phone, email, principal = extracted['phone'], extracted['email'], extracted['principal']
        status = classify_status(phone, email, scrape_result['website_reachable'])

        if not found_website:
            found_website = scrape_result.get('final_url') or target_url

        error_log = "; ".join(errors) if errors else None
        return phone, email, principal, status, search_method, found_website, error_log

    if search_method == 'direct' and website:
        found_url, method = await asyncio.to_thread(find_college_website, college_name, state, district)
        if found_url and found_url != target_url:
            search_method = method
            found_website = found_url
            scrape_result2 = await scrape_college_website(found_url, session)

            if scrape_result2['texts']:
                extracted = await asyncio.to_thread(extract_all, scrape_result2['texts'])
                phone, email, principal = extracted['phone'], extracted['email'], extracted['principal']
                status = classify_status(phone, email, scrape_result2['website_reachable'])
                error_log = "; ".join(errors + scrape_result2['errors']) if (errors or scrape_result2['errors']) else None
                return phone, email, principal, status, search_method, found_website, error_log

    error_log = "; ".join(errors) if errors else "No data could be extracted"
    if scrape_result['website_reachable']:
        return "Not Found", "Not Found", "Not Found", "Inactive", search_method, found_website, error_log
    return "Not Found", "Not Found", "Not Found", "Not Found", search_method, found_website, error_log


async def async_worker(job_id, max_concurrent=50):
    try:
        await asyncio.to_thread(update_job_status, job_id, 'processing')
        await asyncio.to_thread(add_log, job_id, 'INFO', 'Processing started')
        emit_event(job_id, 'status', {'status': 'processing'})

        colleges = await asyncio.to_thread(get_pending_colleges, job_id)
        total = len(colleges)
        if total == 0:
            await asyncio.to_thread(update_job_status, job_id, 'completed')
            emit_event(job_id, 'complete', {'message': 'No colleges to process'})
            return

        emit_event(job_id, 'log', {'level': 'INFO', 'message': f'Found {total} colleges to process'})

        processed, active, inactive, not_found = 0, 0, 0, 0
        start_time = time.time()
        semaphore = asyncio.Semaphore(max_concurrent)
        
        # Connection pooling optimizations for ultra-speed
        connector = aiohttp.TCPConnector(limit=500, ssl=False, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=connector) as session:
            
            async def bound_process(college):
                nonlocal processed, active, inactive, not_found
                async with semaphore:
                    # Check pause/cancel state
                    job_state = active_jobs.get(job_id, {})
                    if job_state.get('cancelled', False):
                        raise asyncio.CancelledError()
                        
                    pause_event = job_state.get('paused')
                    if pause_event and not pause_event.is_set():
                        emit_event(job_id, 'status', {'status': 'paused'})
                        await asyncio.to_thread(pause_event.wait)
                        emit_event(job_id, 'status', {'status': 'processing'})

                    college_name = college.get('college_name', 'Unknown')
                    try:
                        phone, email, principal, status, method, found_website, error_log = await process_single_college(college, session)
                        
                        await asyncio.to_thread(update_college_result, college['id'], phone, email, principal, status, method, found_website, error_log)
                        
                        processed += 1
                        if status == 'Active': active += 1
                        elif status == 'Inactive': inactive += 1
                        else: not_found += 1

                        elapsed = time.time() - start_time
                        rate = processed / elapsed if elapsed > 0 else 0
                        eta = (total - processed) / rate if rate > 0 else 0

                        log_msg = f"[{processed}/{total}] {college_name[:40]}... → {status} | Phone: {phone} | Email: {email}"
                        await asyncio.to_thread(add_log, job_id, 'INFO', log_msg)
                        
                        open_circuits = len([t for t in domain_cooldown.values() if t > time.time()])
                        
                        emit_event(job_id, 'progress', {
                            'processed': processed, 'total': total, 'active': active,
                            'inactive': inactive, 'not_found': not_found, 'current': college_name,
                            'status': status, 'phone': phone, 'email': email, 'principal': principal,
                            'eta_seconds': int(eta), 'rate': round(rate * 60, 1),
                            'connection_stats': {
                                'failed_domains': len(domain_failures),
                                'open_circuits': open_circuits
                            }
                        })
                        emit_event(job_id, 'log', {'level': 'INFO', 'message': log_msg})

                        if processed % 10 == 0:
                            await asyncio.to_thread(update_job_progress, job_id, processed, active, inactive, not_found)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        processed += 1
                        not_found += 1
                        await asyncio.to_thread(add_log, job_id, 'ERROR', f"Error on {college_name}: {str(e)[:200]}")
                        await asyncio.to_thread(update_college_result, college['id'], "Not Found", "Not Found", "Not Found", "Not Found", "error", None, str(e)[:500])

            tasks = [asyncio.create_task(bound_process(c)) for c in colleges]
            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                await asyncio.to_thread(add_log, job_id, 'INFO', 'Processing cancelled by user')
                await asyncio.to_thread(update_job_status, job_id, 'cancelled')
                await asyncio.to_thread(update_job_progress, job_id, processed, active, inactive, not_found)
                emit_event(job_id, 'status', {'status': 'cancelled'})
                return

        await asyncio.to_thread(update_job_progress, job_id, processed, active, inactive, not_found)
        await asyncio.to_thread(update_job_status, job_id, 'completed')
        elapsed = time.time() - start_time
        complete_msg = f"Completed! {processed} colleges processed in {elapsed:.0f}s. Active: {active}, Inactive: {inactive}, Not Found: {not_found}"
        await asyncio.to_thread(add_log, job_id, 'INFO', complete_msg)
        emit_event(job_id, 'complete', {
            'message': complete_msg, 'processed': processed, 'active': active,
            'inactive': inactive, 'not_found': not_found, 'elapsed': int(elapsed),
        })

    except Exception as e:
        error_msg = f"Fatal error: {str(e)}"
        add_log(job_id, 'ERROR', error_msg)
        update_job_status(job_id, 'failed', error_msg)
        emit_event(job_id, 'error', {'message': error_msg})
    finally:
        active_jobs.pop(job_id, None)

def processing_worker(job_id, max_concurrent=50):
    asyncio.run(async_worker(job_id, max_concurrent))




# ─── Generate Output File ────────────────────────────────────────

def generate_output_file(job_id):
    """Generate enriched Excel file from job results."""
    colleges = get_colleges_by_job(job_id)
    job = get_job(job_id)

    if not colleges:
        return None

    rows = []
    for c in colleges:
        rows.append({
            'College Name': c['college_name'],
            'College Type': c['college_type'],
            'State': c['state'],
            'District': c['district'],
            'Website': c['found_website'] or c['original_website'] or '',
            'Contact Number': c['extracted_phone'] or 'Not Found',
            'Mail ID': c['extracted_email'] or 'Not Found',
            'Principal Name': c['extracted_principal'] or 'Not Found',
            'Status': c['status'] or 'Not Found',
            'Search Method': c['search_method'] or '',
        })

    df = pd.DataFrame(rows)
    output_filename = f"enriched_{job.get('filename', 'output')}"
    if not output_filename.endswith('.xlsx'):
        output_filename = output_filename.rsplit('.', 1)[0] + '.xlsx'

    output_path = os.path.join(OUTPUT_DIR, f"{job_id}_{output_filename}")
    df.to_excel(output_path, index=False, engine='openpyxl')
    return output_path


# ─── API Routes ──────────────────────────────────────────────────

@app.route('/')
def serve_frontend():
    return send_from_directory(FRONTEND_DIR, 'index.html')


@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory(FRONTEND_DIR, path)


@app.route('/api/upload', methods=['POST'])
def upload_file():
    """Upload and validate a file, create a processing job."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    if not allowed_file(file.filename):
        return jsonify({'error': f'Invalid file type. Allowed: {", ".join(ALLOWED_EXTENSIONS)}'}), 400

    filename = secure_filename(file.filename)
    filepath = os.path.join(UPLOAD_DIR, f"{uuid.uuid4().hex[:8]}_{filename}")
    file.save(filepath)

    try:
        df = read_upload(filepath)
    except Exception as e:
        os.remove(filepath)
        return jsonify({'error': f'Could not read file: {str(e)}'}), 400

    ok, missing = validate_columns(df)
    if not ok:
        os.remove(filepath)
        return jsonify({'error': f'Missing required columns: {", ".join(missing)}'}), 400

    # Fill NaN with empty strings
    df = df.fillna('')

    # Create job
    total_rows = len(df)
    job_id = create_job(filename, total_rows)

    # Insert all colleges into DB
    colleges_data = df.to_dict('records')
    insert_colleges_batch(job_id, colleges_data)

    add_log(job_id, 'INFO', f'File uploaded: {filename} ({total_rows} rows)')

    # Preview
    preview = df.head(5).to_dict('records')

    return jsonify({
        'job_id': job_id,
        'filename': filename,
        'total_rows': total_rows,
        'columns': list(df.columns),
        'preview': preview,
        'message': f'File uploaded successfully. {total_rows} colleges ready for processing.'
    })


@app.route('/api/start/<job_id>', methods=['POST'])
def start_processing(job_id):
    """Start processing a job."""
    job = get_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404

    if job_id in active_jobs:
        return jsonify({'error': 'Job is already being processed'}), 400

    # Create control events
    pause_event = threading.Event()
    pause_event.set()  # Start unpaused

    active_jobs[job_id] = {
        'paused': pause_event,
        'cancelled': False,
    }
    job_events[job_id] = []

    # Start background thread
    thread = threading.Thread(target=processing_worker, args=(job_id,), daemon=True)
    thread.start()
    active_jobs[job_id]['thread'] = thread

    return jsonify({'message': 'Processing started', 'job_id': job_id})


@app.route('/api/status/<job_id>')
def get_status(job_id):
    """Robust status polling endpoint - replaces fragile SSE."""
    last_log_id = request.args.get('last_log_id', 0, type=int)
    
    job = get_job_summary(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404

    # Fetch new logs
    new_logs = get_logs_since(job_id, last_log_id)
    
    # Get active job state from memory if available
    job_state = active_jobs.get(job_id, {})
    
    # Find the latest progress event for details like ETA, rate
    events = job_events.get(job_id, [])
    latest_progress = next((e['data'] for e in reversed(events) if e['type'] == 'progress'), None)

    return jsonify({
        'job': job,
        'logs': new_logs,
        'progress_details': latest_progress,
        'is_active': job_id in active_jobs,
        'is_paused': job_state.get('paused') and not job_state['paused'].is_set()
    })


# Server Sent Events (SSE) has been deprecated in favor of robust status polling.



@app.route('/api/results/<job_id>')
def get_results(job_id):
    """Get processed results with optional filtering."""
    status_filter = request.args.get('status', 'all')
    search_query = request.args.get('search', '')

    colleges = get_colleges_by_job(job_id, status_filter, search_query)
    job = get_job(job_id)
    counts = get_college_count_by_status(job_id)

    return jsonify({
        'job': job,
        'colleges': colleges,
        'status_counts': counts,
        'total': len(colleges),
    })


@app.route('/api/logs/<job_id>')
def get_job_logs(job_id):
    """Get logs for a job."""
    limit = request.args.get('limit', 200, type=int)
    logs = get_logs(job_id, limit)
    return jsonify({'logs': logs})


@app.route('/api/download/<job_id>')
def download_results(job_id):
    """Download enriched Excel file."""
    job = get_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404

    output_path = generate_output_file(job_id)
    if not output_path:
        return jsonify({'error': 'No results to download'}), 404

    return send_file(
        output_path,
        as_attachment=True,
        download_name=os.path.basename(output_path),
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@app.route('/api/pause/<job_id>', methods=['POST'])
def pause_job(job_id):
    """Pause a running job."""
    if job_id not in active_jobs:
        return jsonify({'error': 'Job is not running'}), 400

    active_jobs[job_id]['paused'].clear()  # Block the worker
    update_job_status(job_id, 'paused')
    add_log(job_id, 'INFO', 'Job paused by user')
    emit_event(job_id, 'status', {'status': 'paused'})

    return jsonify({'message': 'Job paused'})


@app.route('/api/resume/<job_id>', methods=['POST'])
def resume_job(job_id):
    """Resume a paused job."""
    if job_id not in active_jobs:
        # Try to restart from checkpoint
        job = get_job(job_id)
        if job and job['status'] in ('paused', 'failed'):
            return start_processing(job_id)
        return jsonify({'error': 'Job is not running'}), 400

    active_jobs[job_id]['paused'].set()  # Unblock the worker
    update_job_status(job_id, 'processing')
    add_log(job_id, 'INFO', 'Job resumed by user')
    emit_event(job_id, 'status', {'status': 'processing'})

    return jsonify({'message': 'Job resumed'})


@app.route('/api/cancel/<job_id>', methods=['POST'])
def cancel_job(job_id):
    """Cancel a running job."""
    if job_id in active_jobs:
        active_jobs[job_id]['cancelled'] = True
        active_jobs[job_id]['paused'].set()  # Unblock if paused
        add_log(job_id, 'INFO', 'Job cancelled by user')
    else:
        update_job_status(job_id, 'cancelled')

    return jsonify({'message': 'Job cancelled'})


@app.route('/api/jobs')
def list_jobs():
    """List all jobs."""
    from database import get_all_jobs
    jobs = get_all_jobs()
    return jsonify({'jobs': jobs})


# ─── Main ─────────────────────────────────────────────────────────

if __name__ == '__main__':
    print("""
    ╔═══════════════════════════════════════════════════════════════╗
    ║       College Data Enrichment CRM — Ready to Launch!        ║
    ║                                                             ║
    ║   Open your browser:  http://localhost:5000                  ║
    ║                                                             ║
    ╚═══════════════════════════════════════════════════════════════╝
    """)
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)
