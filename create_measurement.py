import argparse
import time
import random
import sys
import shutil
import os


from multiprocessing import Pool
from stations import stations


def create_measurements(records, file_count):
    batch_size = 10_000
    chunk = records // batch_size
    with open(f"weather_measurements_{file_count}.txt", "w") as fp:
        for _ in range(chunk):
            random_stations = random.choices(list(stations), k=batch_size)
            write_string = "\n".join(
                [f"{s};{random.uniform(-99.0, 99.0):.2f}" for s in random_stations]
            )
            fp.write(write_string + "\n")
    return f"weather_measurements_{file_count}.txt"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create measurement file")
    parser.add_argument(
        "-r",
        "--records",
        help="Number of records to create (default is 1_000_000_000)",
        dest="records",
        type=int,
        default=1_000_000_000,
    )

    args = parser.parse_args()
    start_time = time.time()
    temp_file_num = 10
    each_records = args.records // temp_file_num
    with Pool() as pool:
        args_list = [(each_records, i) for i in range(temp_file_num)]
        results = pool.starmap(create_measurements, args_list)

        with open("weather_measurements.txt", "wb") as wfd:
            for f in results:
                with open(f, "rb") as fd:
                    shutil.copyfileobj(fd, wfd)
                os.remove(f)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")
