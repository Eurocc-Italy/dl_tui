import pytest

#
# Testing browse function in api.py library
#

import subprocess
import os
import json
from time import sleep
from zipfile import ZipFile
from conftest import ROOT_DIR

from dlaas.tuilib.common import Config
from dlaas.tuilib.api import upload, delete, download, browse

# loading IP and token from defaults
ip = Config("hpc").ip
with open(f"{os.environ['HOME']}/.config/dlaas/api-token.txt", "r") as f:
    token = f.read()


def check_status(job_id):
    """Checks that results have been uploaded to the data lake, and then download the results archive

    Parameters
    ----------
    job_id : str
        job identifier for file downloads
    """
    while True:
        sleep(1)
        # checking that results file has been uploaded
        response = browse(ip=ip, token=token, filter=f"job_id = {job_id}").text

        if f"results_{job_id}.zip" in response:
            # download results archive
            sleep(5)  # wait for the download to be available
            download(ip=ip, token=token, file=f"results_{job_id}.zip")
            sleep(5)  # wait for the download to be completed
            break


@pytest.fixture(scope="function", autouse=True)
def setup_testfiles():

    # creating test files
    with open("test.txt", "w") as f:
        f.write("test")
    with open("test2.txt", "w") as f:
        f.write("test2")
    with open("query.txt", "w") as f:
        f.write("SELECT * FROM metadata WHERE category = dog OR category = cat")
    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]")

    # creating metadata for test files
    with open("test.json", "w") as f:
        json.dump({"category": "dog"}, f)
    with open("test2.json", "w") as f:
        json.dump({"category": "cat"}, f)

    # run tests
    yield

    # delete local test files
    os.remove("test.txt")
    os.remove("test.json")
    os.remove("test2.txt")
    os.remove("test2.json")
    os.remove("query.txt")

    # delete files on data lake
    delete(ip=ip, token=token, file="test.txt")
    delete(ip=ip, token=token, file="test2.txt")


def test_upload():
    """
    Upload a file
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --upload --file=test.txt --metadata=test.json",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b"Successfully uploaded file test.txt.\n"
    assert stderr == b""


def test_upload_again():
    """
    Upload same file twice
    """

    # Upload file preemptively
    upload(ip=ip, token=token, file="test.txt", json_data="test.json")

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --upload --file=test.txt --metadata=test.json",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b'"Upload Failed, entry is already present. Please use PUT method to update an existing entry"\n\n'
    assert b"400 Client Error: BAD REQUEST for url: http://131.175.205.87:8080/v1/upload" in stderr


def test_download():
    """
    Download a file
    """

    # Upload file for download
    upload(ip=ip, token=token, file="test.txt", json_data="test.json")

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --download --key=test.txt",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b"Successfully downloaded file test.txt.\n"
    assert stderr == b""


def test_download_nonexistent():
    """
    Download a file not in the data lake
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --download --key=test-nonexistent.txt",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b"File not found\n\n"
    assert (
        b"404 Client Error: NOT FOUND for url: http://131.175.205.87:8080/v1/download?file_name=test-nonexistent.txt"
        in stderr
    )


def test_query_search_only():
    """
    Launch query on Data Lake (only query)
    """

    # upload test files if not present
    upload(ip=ip, token=token, file="test.txt", json_data="test.json")
    upload(ip=ip, token=token, file="test2.txt", json_data="test2.json")

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --query --query_file=query.txt",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    job_id = str(stdout).split()[-1].replace("\\n'", "")
    assert len(job_id) == 32

    assert stdout[:89] == b"Successfully launched query SELECT * FROM metadata WHERE category = dog OR category = cat"
    assert stderr == b""

    check_status(job_id=job_id)

    # checking results archive
    assert os.path.exists(f"results_{job_id}.zip"), "Zipped archive was not created."

    with ZipFile(f"results_{job_id}.zip", "r") as archive:
        archive = archive.namelist()
        archive.sort()
        assert archive == [
            f"query_{job_id}.txt",
            "test.txt",
            "test2.txt",
        ], "Results archive does not contain the expected files."


def test_query_with_script():
    """
    Launch query on Data Lake (only query)
    """

    # upload test files if not present
    upload(ip=ip, token=token, file="test.txt", json_data="test.json")
    upload(ip=ip, token=token, file="test2.txt", json_data="test2.json")

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --query --query_file=query.txt --python_file=user_script.py",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    job_id = str(stdout).split()[-1].replace("\\n'", "")
    assert len(job_id) == 32

    assert (
        stdout[:123]
        == b"Successfully launched analysis script user_script.py on query SELECT * FROM metadata WHERE category = dog OR category = cat"
    )
    assert stderr == b""

    check_status(job_id=job_id)

    # checking results archive
    assert os.path.exists(f"results_{job_id}.zip"), "Zipped archive was not created."

    with ZipFile(f"results_{job_id}.zip", "r") as archive:
        archive = archive.namelist()
        archive.sort()
        slurm_job = int(archive[1].replace("slurm-", "").replace(".out", ""))
        assert archive == [
            f"query_{job_id}.txt",
            f"slurm-{slurm_job}.out",
            f"slurm-{slurm_job+1}.out",
            "test.txt",
            f"user_script_{job_id}.py",
        ], "Results archive does not contain the expected files."


def test_replace():
    """
    Replace a file
    """

    # Upload file for replacement
    upload(ip=ip, token=token, file="test.txt", json_data="test.json")

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --replace --file=test.txt --metadata=test.json",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b"Successfully replaced file test.txt.\n"
    assert stderr == b""


def test_replace_nonexistent():
    """
    Replace a file not on the datalake
    """

    # Delete file if existing
    delete(ip=ip, token=token, file="test2.txt")

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --replace --file=test2.txt --metadata=test2.json",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b'"Replacement failed, file not found. Please use POST method to create a new entry"\n\n'
    assert b"400 Client Error: BAD REQUEST for url: http://131.175.205.87:8080/v1/replace" in stderr


def test_update():
    """
    Update a file
    """

    # Upload file for update
    upload(ip=ip, token=token, file="test.txt", json_data="test.json")

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --update --key=test.txt --metadata=test2.json",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b"Successfully updated metadata for file test.txt.\n"
    assert stderr == b""


def test_update_nonexistent():
    """
    Update a file not on the datalake
    """

    # Delete file if existing
    delete(ip=ip, token=token, file="test2.txt")

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --update --key=test2.txt --metadata=test2.json",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b'"Update failed, file not found. Please use POST method to create a new entry"\n\n'
    assert b"400 Client Error: BAD REQUEST for url: http://131.175.205.87:8080/v1/update" in stderr


def test_browse_all():
    """
    Browse all files
    """

    # Upload files for browsing
    upload(ip=ip, token=token, file="test.txt", json_data="test.json")

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --browse",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b"Filter: None\nFiles:\n  - test.txt\n"
    assert stderr == b""


def test_browse_filter():
    """
    Browse data lake using filters
    """

    # Upload files for browsing
    upload(ip=ip, token=token, file="test.txt", json_data="test.json")

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --browse --filter='category = dog'",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b"Filter: category = dog\nFiles:\n  - test.txt\n"
    assert stderr == b""


def test_delete():
    """
    Delete a file
    """

    # Upload files for deletion
    upload(ip=ip, token=token, file="test.txt", json_data="test.json")

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --delete --key=test.txt",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b"Successfully deleted file test.txt.\n"
    assert stderr == b""


def test_delete_nonexistent():
    """
    Delete a file not in the data lake
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --delete --key=test2.txt",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b'"File path not found in the database"\n\n'
    assert b"404 Client Error: NOT FOUND for url: http://131.175.205.87:8080/v1/delete?file_name=test2.txt" in stderr


def test_wrong_ip():
    """
    Browse using the wrong IP
    """

    with pytest.raises(subprocess.TimeoutExpired):
        stdout, stderr = subprocess.Popen(
            f"{ROOT_DIR}/dlaas/bin/dl_tui.py --browse --ip=wrong.ip.com",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ).communicate(timeout=2)


def test_upload_missing_file():
    """
    Run upload action without --file parameter
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --upload --metadata=test.json",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b""
    assert stderr[-49:] == b"KeyError: 'Required argument is missing: --file'\n"


def test_upload_missing_metadata():
    """
    Run upload action without --metadata parameter
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --upload --file=test.txt",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b""
    assert stderr[-53:] == b"KeyError: 'Required argument is missing: --metadata'\n"


def test_replace_missing_file():
    """
    Run replace action without --file parameter
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --replace --metadata=test.json",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b""
    assert stderr[-49:] == b"KeyError: 'Required argument is missing: --file'\n"


def test_replace_missing_metadata():
    """
    Run replace action without --metadata parameter
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --replace --file=test.txt",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b""
    assert stderr[-53:] == b"KeyError: 'Required argument is missing: --metadata'\n"


def test_update_missing_file():
    """
    Run update action without --key parameter
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --update --metadata=test.json",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b""
    assert stderr[-48:] == b"KeyError: 'Required argument is missing: --key'\n"


def test_update_missing_metadata():
    """
    Run update action without --metadata parameter
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --update --key=test.txt",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b""
    assert stderr[-53:] == b"KeyError: 'Required argument is missing: --metadata'\n"


def test_download_missing_metadata():
    """
    Run download action without --key parameter
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --download",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b""
    assert stderr[-48:] == b"KeyError: 'Required argument is missing: --key'\n"


def test_delete_missing_metadata():
    """
    Run delete action without --key parameter
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --delete",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b""
    assert stderr[-48:] == b"KeyError: 'Required argument is missing: --key'\n"


def test_query_missing_metadata():
    """
    Run query action without --query_file parameter
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py --query",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b""
    assert stderr[-55:] == b"KeyError: 'Required argument is missing: --query_file'\n"
