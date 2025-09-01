import os
import time
from django.conf import settings
from django.shortcuts import render
from django.core.files.storage import FileSystemStorage
from schemabridge.snowflakeToTalend import main as talend_main


from datetime import datetime
import glob
def clean_upload_dirs():
    for folder in ['uploads', 'xml_outputs']:
        dir_path = os.path.join(settings.MEDIA_ROOT, folder)
        for f in glob.glob(os.path.join(dir_path, '*')):
            try:
                os.remove(f)
            except Exception:
                pass

def home(request):
    clean_upload_dirs()  # Clean old files on every page load
    xml_result = None
    xml_download_url = None
    debug_error = None
    parsed_columns = None
    analysis_stats = None
    process_time = None
    if request.method == 'POST' and request.FILES.get('excel_file'):
        excel_file = request.FILES['excel_file']
        file_ext = os.path.splitext(excel_file.name)[1].lower()
        # File size limit: 5MB
        if excel_file.size > 5 * 1024 * 1024:
            debug_error = '[ERROR] File size exceeds 5MB limit.'
        elif file_ext not in ['.xlsx', '.csv']:
            debug_error = '[ERROR] Only .xlsx and .csv files are supported.'
        else:
            fs = FileSystemStorage(location=os.path.join(settings.MEDIA_ROOT, 'uploads'))
            filename = fs.save(excel_file.name, excel_file)
            file_path = fs.path(filename)
            # Purposefully wait 2 seconds for UI effect
            time.sleep(2)
            # Prepare unique output XML path
            base_name = os.path.splitext(os.path.basename(filename))[0]
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            xml_filename = f"{base_name}_talend_{timestamp}.xml"
            xml_output_path = os.path.join(settings.MEDIA_ROOT, 'xml_outputs', xml_filename)
            # Call the main logic
            if hasattr(talend_main, 'process_excel_schema'):
                try:
                    start_time = time.time()
                    xml_result, log_list = talend_main.process_excel_schema(file_path, xml_output_path)
                    end_time = time.time()
                    process_time = end_time - start_time
                    error_logs = [log for log in log_list if log.startswith('[ERROR]') or log.startswith('[WARNING]')]
                    if error_logs:
                        debug_error = '\n'.join(error_logs)
                    # Extract parsed columns for summary table
                    import pandas as pd
                    df, _ = talend_main.read_schema(file_path)
                    if df is not None:
                        parsed_columns = []
                        for _, row in df.iterrows():
                            talend_type, _, _, _ = talend_main.parse_type(row['Column_Type'])
                            parsed_columns.append({
                                'Column_Name': row['Column_Name'],
                                'Column_Type': row['Column_Type'],
                                'Talend_Type': talend_type
                            })
                        # Analysis stats
                        analysis_stats = {
                            'record_count': len(df),
                            'file_size': os.path.getsize(file_path),
                            'file_name': os.path.basename(file_path),
                            'process_time': process_time
                        }
                except Exception as e:
                    debug_error = f'[ERROR] {str(e)}'
                xml_download_url = settings.MEDIA_URL + 'xml_outputs/' + os.path.basename(xml_output_path) if xml_result else None
            else:
                debug_error = '[ERROR] Processing function not implemented.'
    return render(request, 'site/home.html', {
        'xml_result': xml_result,
        'xml_download_url': xml_download_url,
        'debug_error': debug_error,
        'parsed_columns': parsed_columns,
        'analysis_stats': analysis_stats
    })
