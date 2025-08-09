import pandas as pd
import sys
from subprocess import check_call
import os
import time

start_time = time.time()

# Choose python executable name
pvar = "python" if sys.platform == "win32" else "python3"

# Hadoop commands
dfs = "dfs"
hdfs = "hdfs"


def clean_amenities(text: str) -> str:
    replacements = {
        "Wifi \\u2013 600 Mbps": "Wifi",
        "Wifi \\u2013 100 Mbps": "Wifi",
        "Paid street parking off premises": "Paid Off",
        "Paid parking off premises": "Paid Off",
        "Paid parking lot off premise": "Paid Off",
        "Paid parking garage off premises": "Paid Off",
        "Washer \\u2013\\u00a0In unit": "Washer",
        "Dryer \\u2013\\u00a0In unit": "Dryer",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


# Create cleaned dataset
df = pd.read_csv("listings_detailed.csv")
df["amenities"] = df["amenities"].apply(clean_amenities)
df[["amenities"]].to_csv("dataset.csv", index=False, header=False)


def run_hadoop():
    try:
        # Clean and prepare HDFS
        for path in ["/tmp", "/user/hduser/tmp"]:
            check_call([hdfs, dfs, "-rm", "-r", "-f", path])
        check_call([hdfs, dfs, "-mkdir", "/tmp/tomrock"])
        check_call([hdfs, dfs, "-put", "-f", "dataset.csv", "/tmp/tomrock/dataset.csv"])

        # Run MRJob on Hadoop
        check_call(
            [
                pvar,
                "MRJobWrapper.py",
                sys.argv[2],
                sys.argv[3],
                "-r",
                "hadoop",
                "hdfs://hadoop-master:54310/tmp/tomrock/dataset.csv",
                sys.argv[4],
            ]
        )
    finally:
        # Cleanup
        for path in ["/user/hduser/tmp", "/tmp"]:
            check_call([hdfs, dfs, "-rm", "-r", "-f", path])


def run_local():
    venv_path = os.path.dirname(sys.executable)
    check_call(
        [
            os.path.join(venv_path, pvar),
            "MRJobWrapper.py",
            sys.argv[2],
            sys.argv[3],
            "-r",
            "inline",
            "dataset.csv",
            sys.argv[4],
        ]
    )


# Main execution
if sys.argv[1] == "hadoop":
    run_hadoop()
else:
    run_local()

os.remove("varx.json")
os.remove("dataset.csv")

print(f"Time Taken: {round(time.time() - start_time, 2)}s")
