"""Notebook execution and orchestration utilities for Fabric."""

import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from notebookutils import mssparkutils


def run_notebook_with_tracking(notebook_name, notebook_path, timeout_seconds=1800, parameters=None):
    """Executes a notebook and tracks its execution time and status."""
    start_time = datetime.utcnow()
    result = {
        'notebook_name': notebook_name,
        'notebook_path': notebook_path,
        'parameters': parameters,
        'start_time': start_time,
        'status': 'FAILED',
        'duration_seconds': 0,
        'error': None
    }

    try:
        print(f"\n{'='*80}")
        print(f"Starting: {notebook_name}")
        print(f"Path: {notebook_path}")
        print(f"Timeout: {timeout_seconds}s")
        if parameters:
            print(f"Parameters: {json.dumps(parameters, indent=2)}")
        print(f"{'='*80}\n")

        if parameters:
            mssparkutils.notebook.run(notebook_path, timeout_seconds, parameters)
        else:
            mssparkutils.notebook.run(notebook_path, timeout_seconds)

        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        result['status'] = 'SUCCESS'
        result['end_time'] = end_time
        result['duration_seconds'] = duration

    except Exception as e:
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        result['end_time'] = end_time
        result['duration_seconds'] = duration
        result['error'] = str(e)

    return result


def run_notebooks_sequential(jobs):
    """Executes notebooks sequentially, respecting dependencies."""
    results = []
    for job in jobs:
        result = run_notebook_with_tracking(
            notebook_name=job['name'],
            notebook_path=job['path'],
            timeout_seconds=job['timeout'],
            parameters=job.get('parameters')
        )
        results.append(result)
        if result['status'] == 'FAILED':
            dependent_jobs = [j for j in jobs if job['name'] in j.get('depends_on', [])]
            if dependent_jobs:
                for dep_job in dependent_jobs:
                    results.append({
                        'notebook_name': dep_job['name'],
                        'notebook_path': dep_job['path'],
                        'start_time': datetime.utcnow(),
                        'end_time': datetime.utcnow(),
                        'status': 'SKIPPED',
                        'duration_seconds': 0,
                        'error': f"Skipped due to failed dependency: {job['name']}"
                    })
    return results


def run_notebooks_parallel(jobs, max_workers=3):
    """Executes notebooks in parallel, respecting dependencies."""
    results = []
    batch_1 = [job for job in jobs if not job.get('depends_on')]
    batch_2 = [job for job in jobs if job.get('depends_on')]

    if batch_1:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    run_notebook_with_tracking,
                    job['name'], job['path'], job['timeout'], job.get('parameters')
                ): job for job in batch_1
            }
            for future in as_completed(futures):
                results.append(future.result())

    if batch_2:
        batch_1_failed = [r for r in results if r['status'] == 'FAILED']
        if batch_1_failed:
            for job in batch_2:
                results.append({
                    'notebook_name': job['name'],
                    'notebook_path': job['path'],
                    'start_time': datetime.utcnow(),
                    'end_time': datetime.utcnow(),
                    'status': 'SKIPPED',
                    'duration_seconds': 0,
                    'error': 'Skipped due to failed dependencies in Batch 1'
                })
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        run_notebook_with_tracking,
                        job['name'], job['path'], job['timeout'], job.get('parameters')
                    ): job for job in batch_2
                }
                for future in as_completed(futures):
                    results.append(future.result())

    return results


def print_execution_summary(results, orchestration_id, execution_mode):
    """Prints a formatted summary of all notebook executions."""
    print("\n" + "="*100)
    print("ORCHESTRATION SUMMARY")
    print("="*100)

    total_duration = sum(r.get('duration_seconds', 0) or 0 for r in results)
    successful = [r for r in results if r.get('status') == 'SUCCESS']
    failed = [r for r in results if r.get('status') == 'FAILED']

    print(f"\nOrchestration ID: {orchestration_id}")
    print(f"Execution Mode: {execution_mode}")
    print(f"Total Notebooks: {len(results)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    print(f"Total Duration: {total_duration:.2f}s ({total_duration/60:.2f} minutes)")

    print("\n" + "-"*100)
    print(f"{'Notebook':<50} {'Status':<10} {'Duration':<15} {'Error':<25}")
    print("-"*100)

    for result in results:
        notebook_name = result.get('notebook_name') or 'Unknown'
        status = result.get('status') or 'UNKNOWN'
        duration = result.get('duration_seconds') or 0
        error_str = result.get('error') or ''
        if error_str:
            error_str = error_str[:22] + "..." if len(error_str) > 25 else error_str
        print(f"{notebook_name:<50} {status:<10} {duration:.2f}s{'':<10} {error_str:<25}")

    print("="*100 + "\n")
