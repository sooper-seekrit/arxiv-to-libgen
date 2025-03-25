#!/usr/bin/env python3
"""
ArXiv to LibGen Backup - Coordination Server

This standalone server coordinates multiple instances of the ArXiv to LibGen backup tool
to prevent duplicate downloads and uploads. It tracks which papers are being processed
by which instance and provides a RESTful API for coordination.

Usage:
  python coordination_server.py --host 0.0.0.0 --port 5000 --debug
"""

import argparse
import json
import time
import logging
import os
from datetime import datetime
from flask import Flask, request, jsonify, render_template

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("coordination_server.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("coordination_server")

# Create Flask app
app = Flask(__name__)

# In-memory storage for paper status
# Structure: {arxiv_id: {status, timestamp, instance_id, ...}}
papers_status = {}

# Constants
STATUS_PROCESSING = 'processing'
STATUS_COMPLETED = 'completed'
STATUS_FAILED = 'failed'
STATUS_TIMEOUT = 'timeout'

# Timeout for processing (in seconds)
PROCESSING_TIMEOUT = 3600  # 1 hour

# File to periodically save state
STATE_FILE = 'coordination_state.json'

# HTML template for the status page
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>ArXiv Backup Coordination Status</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #f2f2f2;
            font-weight: bold;
        }
        tr:hover {
            background-color: #f9f9f9;
        }
        .status-processing {
            color: blue;
        }
        .status-completed {
            color: green;
        }
        .status-failed {
            color: red;
        }
        .status-timeout {
            color: orange;
        }
        .summary {
            margin-top: 20px;
            padding: 15px;
            background-color: #f2f2f2;
            border-radius: 5px;
        }
        .refresh {
            float: right;
            margin-top: 10px;
        }
        @media (max-width: 768px) {
            table {
                font-size: 14px;
            }
            th, td {
                padding: 8px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>ArXiv Backup Coordination Status</h1>
        <button class="refresh" onclick="location.reload()">Refresh</button>
        
        <div class="summary">
            <p><strong>Total Papers:</strong> {{total_papers}}</p>
            <p><strong>Processing:</strong> {{processing_count}} | 
               <strong>Completed:</strong> {{completed_count}} | 
               <strong>Failed:</strong> {{failed_count}} | 
               <strong>Timeout:</strong> {{timeout_count}}</p>
            <p><strong>Last Updated:</strong> {{last_updated}}</p>
        </div>
        
        <table>
            <tr>
                <th>ArXiv ID</th>
                <th>Status</th>
                <th>Instance</th>
                <th>Started</th>
                <th>Updated</th>
                <th>Duration</th>
            </tr>
            {% for id, data in papers %}
            <tr>
                <td><a href="https://arxiv.org/abs/{{id}}" target="_blank">{{id}}</a></td>
                <td class="status-{{data.status}}">{{data.status}}</td>
                <td>{{data.instance_id|default('Unknown')}}</td>
                <td>{{data.start_time|default('')}}</td>
                <td>{{data.update_time|default('')}}</td>
                <td>{{data.duration|default('')}}</td>
            </tr>
            {% endfor %}
        </table>
    </div>
</body>
</html>
"""


def check_timeouts():
    """Check for papers that have been processing for too long."""
    current_time = time.time()
    timeout_count = 0
    
    for arxiv_id, data in papers_status.items():
        if data['status'] == STATUS_PROCESSING:
            # Check if processing has timed out
            if current_time - data['timestamp'] > PROCESSING_TIMEOUT:
                data['status'] = STATUS_TIMEOUT
                data['timeout_timestamp'] = current_time
                timeout_count += 1
    
    if timeout_count > 0:
        logger.info(f"Marked {timeout_count} papers as timed out")


def save_state():
    """Save the current state to a file."""
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(papers_status, f)
        logger.info(f"Saved state to {STATE_FILE}")
    except Exception as e:
        logger.error(f"Error saving state: {e}")


def load_state():
    """Load state from file if it exists."""
    global papers_status
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r') as f:
                papers_status = json.load(f)
            logger.info(f"Loaded state from {STATE_FILE} ({len(papers_status)} papers)")
    except Exception as e:
        logger.error(f"Error loading state: {e}")


@app.route('/')
def index():
    """Display a human-readable status page."""
    # Check for timeouts
    check_timeouts()
    
    # Prepare data for the template
    papers_list = sorted(
        [(arxiv_id, data) for arxiv_id, data in papers_status.items()],
        key=lambda x: x[1]['timestamp'],
        reverse=True
    )
    
    # Process timestamps into readable format
    for arxiv_id, data in papers_list:
        # Format timestamps
        if 'timestamp' in data:
            start_time = datetime.fromtimestamp(data['timestamp'])
            data['start_time'] = start_time.strftime('%Y-%m-%d %H:%M:%S')
        
        if 'update_timestamp' in data:
            update_time = datetime.fromtimestamp(data['update_timestamp'])
            data['update_time'] = update_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Calculate duration
        if 'timestamp' in data:
            end_time = data.get('update_timestamp', time.time())
            duration_secs = end_time - data['timestamp']
            minutes, seconds = divmod(int(duration_secs), 60)
            hours, minutes = divmod(minutes, 60)
            
            if hours > 0:
                data['duration'] = f"{hours}h {minutes}m {seconds}s"
            elif minutes > 0:
                data['duration'] = f"{minutes}m {seconds}s"
            else:
                data['duration'] = f"{seconds}s"
    
    # Count papers by status
    processing_count = sum(1 for data in papers_status.values() if data['status'] == STATUS_PROCESSING)
    completed_count = sum(1 for data in papers_status.values() if data['status'] == STATUS_COMPLETED)
    failed_count = sum(1 for data in papers_status.values() if data['status'] == STATUS_FAILED)
    timeout_count = sum(1 for data in papers_status.values() if data['status'] == STATUS_TIMEOUT)
    
    # Render template directly
    return HTML_TEMPLATE.replace("{{total_papers}}", str(len(papers_status)))\
                       .replace("{{processing_count}}", str(processing_count))\
                       .replace("{{completed_count}}", str(completed_count))\
                       .replace("{{failed_count}}", str(failed_count))\
                       .replace("{{timeout_count}}", str(timeout_count))\
                       .replace("{{last_updated}}", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))\
                       .replace("{% for id, data in papers %}", "")\
                       .replace("{% endfor %}", "")\
                       .replace("{{data.status}}", "")\
                       .replace("{{data.instance_id|default('Unknown')}}", "")\
                       .replace("{{data.start_time|default('')}}", "")\
                       .replace("{{data.update_time|default('')}}", "")\
                       .replace("{{data.duration|default('')}}", "")\
                       .replace("{{id}}", "")


@app.route('/register', methods=['POST'])
def register_paper():
    """Register a paper as being processed by an instance."""
    data = request.json
    arxiv_id = data.get('arxiv_id')
    instance_id = data.get('instance_id', 'unknown')
    
    if not arxiv_id:
        return jsonify({"status": "error", "message": "No arxiv_id provided"}), 400
    
    # Check if paper is already being processed
    if arxiv_id in papers_status:
        paper_data = papers_status[arxiv_id]
        
        # If already being processed and not timed out
        if (paper_data['status'] == STATUS_PROCESSING and 
            time.time() - paper_data['timestamp'] < PROCESSING_TIMEOUT):
            # If the same instance is re-registering, allow it
            if paper_data.get('instance_id') == instance_id:
                paper_data['timestamp'] = time.time()  # Refresh timestamp
                logger.info(f"Re-registered {arxiv_id} for instance {instance_id}")
                return jsonify({"status": "ok"})
            else:
                # Different instance trying to process the same paper
                logger.info(f"Rejected registration of {arxiv_id} - already being processed by {paper_data.get('instance_id')}")
                return jsonify({
                    "status": "error", 
                    "message": f"Already being processed by instance {paper_data.get('instance_id')}"
                }), 409
        
        # If completed, failed, or timed out, allow re-processing
        if paper_data['status'] in [STATUS_COMPLETED, STATUS_FAILED, STATUS_TIMEOUT]:
            logger.info(f"Re-processing {arxiv_id} (previous status: {paper_data['status']})")
    
    # Register the paper
    current_time = time.time()
    papers_status[arxiv_id] = {
        'status': STATUS_PROCESSING,
        'timestamp': current_time,
        'instance_id': instance_id
    }
    
    logger.info(f"Registered {arxiv_id} for processing by instance {instance_id}")
    
    # Periodically save state (every 10 registrations)
    if len(papers_status) % 10 == 0:
        save_state()
    
    return jsonify({"status": "ok"})


@app.route('/complete', methods=['POST'])
def complete_paper():
    """Mark a paper as completed or failed."""
    data = request.json
    arxiv_id = data.get('arxiv_id')
    success = data.get('success', False)
    instance_id = data.get('instance_id')
    
    if not arxiv_id:
        return jsonify({"status": "error", "message": "No arxiv_id provided"}), 400
    
    # Update paper status
    current_time = time.time()
    
    if arxiv_id in papers_status:
        # Update existing entry
        papers_status[arxiv_id]['status'] = STATUS_COMPLETED if success else STATUS_FAILED
        papers_status[arxiv_id]['update_timestamp'] = current_time
        
        # Only update instance_id if provided
        if instance_id:
            papers_status[arxiv_id]['instance_id'] = instance_id
    else:
        # Create new entry
        papers_status[arxiv_id] = {
            'status': STATUS_COMPLETED if success else STATUS_FAILED,
            'timestamp': current_time,
            'update_timestamp': current_time,
            'instance_id': instance_id
        }
    
    logger.info(f"Marked {arxiv_id} as {'completed' if success else 'failed'} by instance {instance_id}")
    
    # Periodically save state (every 10 completions)
    if len(papers_status) % 10 == 0:
        save_state()
    
    return jsonify({"status": "ok"})


@app.route('/status', methods=['GET'])
def get_status():
    """Get the status of all papers or a specific paper."""
    arxiv_id = request.args.get('arxiv_id')
    
    # Check for timeouts
    check_timeouts()
    
    if arxiv_id:
        # Return status for specific paper
        if arxiv_id in papers_status:
            return jsonify({arxiv_id: papers_status[arxiv_id]})
        else:
            return jsonify({}), 404
    else:
        # Return all statuses
        return jsonify(papers_status)


@app.route('/summary', methods=['GET'])
def get_summary():
    """Get a summary of paper processing status."""
    # Check for timeouts
    check_timeouts()
    
    # Count papers by status
    processing_count = sum(1 for data in papers_status.values() if data['status'] == STATUS_PROCESSING)
    completed_count = sum(1 for data in papers_status.values() if data['status'] == STATUS_COMPLETED)
    failed_count = sum(1 for data in papers_status.values() if data['status'] == STATUS_FAILED)
    timeout_count = sum(1 for data in papers_status.values() if data['status'] == STATUS_TIMEOUT)
    
    summary = {
        'total': len(papers_status),
        'processing': processing_count,
        'completed': completed_count,
        'failed': failed_count,
        'timeout': timeout_count,
        'success_rate': completed_count / (completed_count + failed_count + timeout_count) * 100 if (completed_count + failed_count + timeout_count) > 0 else 0
    }
    
    return jsonify(summary)


@app.route('/reset', methods=['POST'])
def reset_status():
    """Reset the status of a paper or all papers (admin function)."""
    data = request.json
    arxiv_id = data.get('arxiv_id')
    admin_key = data.get('admin_key')
    
    # Very basic security check
    if admin_key != os.environ.get('ADMIN_KEY'):
        return jsonify({"status": "error", "message": "Unauthorized"}), 401
    
    if arxiv_id:
        # Reset specific paper
        if arxiv_id in papers_status:
            del papers_status[arxiv_id]
            logger.info(f"Reset status for {arxiv_id}")
        else:
            return jsonify({"status": "error", "message": "ArXiv ID not found"}), 404
    else:
        # Reset all papers
        papers_status.clear()
        logger.info("Reset all paper statuses")
    
    # Save state after reset
    save_state()
    
    return jsonify({"status": "ok"})


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='ArXiv to LibGen Backup Coordination Server')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='Port to listen on')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--admin-key', help='Admin key for reset functionality')
    
    args = parser.parse_args()
    
    # Set admin key if provided
    if args.admin_key:
        os.environ['ADMIN_KEY'] = args.admin_key
    
    # Load saved state if available
    load_state()
    
    # Start the Flask app
    logger.info(f"Starting coordination server on {args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
