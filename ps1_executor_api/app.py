from flask import Flask, request, jsonify
import subprocess

app = Flask(__name__)


@app.route('/run-sql-script', methods=['POST'])
def run_sql_script():
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


@app.route('/run-ps1-script', methods=['POST'])
def run_ps1_script():
    data = request.json
    script_name = data.get('script_name')

    if not script_name:
        return jsonify({'error': 'No script name provided'}), 400

    # Ensure that the script name ends in .ps1 for security
    if not script_name.endswith('.ps1'):
        return jsonify({'error': 'Invalid script type'}), 400

    try:
        # Execute the PowerShell script
        completed_process = subprocess.run(
            ["powershell", "-File", f"..\\powershell_scripts\\{script_name}"],
            capture_output=True,
            text=True,
            check=True
        )

        return jsonify({
            'message': 'Script executed successfully',
            'stdout': completed_process.stdout,
            'stderr': completed_process.stderr
        }), 200

    except subprocess.CalledProcessError as e:
        return jsonify({
            'error': 'Script execution failed',
            'stderr': e.stderr
        }), 500


if __name__ == '__main__':
    app.run(debug=True)
