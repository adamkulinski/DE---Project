from flask import Flask, request, jsonify
import subprocess

app = Flask(__name__)


@app.route('/runscript', methods=['POST'])
def run_script():
    # Get the SQL script name from the POST request
    data = request.json
    sql_script_name = data.get('sql_script_name')

    if not sql_script_name:
        return jsonify({'error': 'No SQL script name provided'}), 400

    try:
        # Execute the PowerShell script with the SQL script name as a parameter
        completed_process = subprocess.run(
            ["powershell", "-File", "..\\powershell_scripts\\RunSqlScript.ps1", "-sql_script_name",
             sql_script_name],
            capture_output=True, text=True, check=True
        )

        return jsonify({
            'message': 'PowerShell script executed successfully',
            'stdout': completed_process.stdout,
            'stderr': completed_process.stderr
        }), 200

    except subprocess.CalledProcessError as e:
        return jsonify({
            'error': 'PowerShell script execution failed',
            'stderr': e.stderr
        }), 500


if __name__ == '__main__':
    app.run(debug=True)
