from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
import apache_beam.runners.interactive.interactive_beam as ib
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from shapely.geometry import Point
from geodatasets import get_path
import numpy as np
import pandas as pd
from zipfile import ZipFile
import geopandas as gpd

import imageio
import matplotlib.pyplot as plt
import shutil
import os
import hashlib
import csv


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'noaa_dataprocessing_dag',
    default_args=default_args,
    schedule_interval='*/1 * * * *',  # set to run every 1 minute
)

archive_dir = "/root/airflow/logs/"
unzip_dir = "/root/airflow/logs/archive/"
output_dir = "/root/airflow/logs/result/"
required_fields = ["HourlyWindSpeed",
            "HourlyDryBulbTemperature"] #Add more fields if needed

# Use FileSensor with timeout
wait_for_archive = FileSensor(
    task_id='wait_for_archive',
    filepath='/root/airflow/logs/archive.zip',
    poke_interval=5,
    timeout=5,  # timeout after 5 seconds
    mode='poke',
    dag=dag,
)

def unzip_files():
    archive_path = f"{archive_dir}archive.zip"
    destination_dir = unzip_dir
    if os.path.exists(unzip_dir):
        shutil.rmtree(unzip_dir)
    os.makedirs(unzip_dir)
    with ZipFile(archive_path, 'r') as zip_ref:
        zip_ref.extractall(destination_dir)

unzip_files_task = PythonOperator(
    task_id='unzip_files',
    python_callable=unzip_files,
    dag=dag,
)

def process_task():
    def process_csv(csv_file):
        csv_path = os.path.join(unzip_dir, csv_file)
        df = pd.read_csv(csv_path)
        # Extract required fields
        # Drop rows with NaN values in the required fields
        df = df.dropna(subset=required_fields)
        # Filter the DataFrame based on the required fields
        filtered_df = df[["DATE"] + required_fields]
        # Extract Lat/Long values
        lat_lon = (df["LATITUDE"].iloc[0], df["LONGITUDE"].iloc[0])
        # Create a tuple of the form <Lat, Lon, [[Date, Windspeed, BulbTemperature, RelativeHumidity, WindDirection], ...]>
        return lat_lon, filtered_df.values.tolist()

    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        # Read from CSV file
        pcollection = (
            pipeline
            | "Read csv files in folder" >> beam.Create(os.listdir(unzip_dir))
            | "Parse CSV" >> beam.Map(process_csv)
            | "Write output"
            >> beam.io.WriteToText(os.path.join(output_dir, "result.txt"))
        )


def read_text_files(file_pattern):
    with open(file_pattern, "r") as file:
        lines = file.readlines()
        return [line.strip() for line in lines]


def monthly_average_task():
    output_file_path = "/root/airflow/logs/result/averages.txt"

    def compute_average(line):
        data_tuple = eval(
            line.strip()
        )  # Assuming the tuple format is preserved in the result.txt file
        lat_lon = data_tuple[0]
        data_list = data_tuple[1]

        monthly_averages = {}
        for data_point in data_list:
            date_str, windspeed, bulb_temp = data_point
            month = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m")

            if month not in monthly_averages:
                monthly_averages[month] = {
                    "AverageWindSpeed": [],
                    "AverageDryBulbTemperature": [],
                }

            # Append values for each field, ignoring NaN values
            if not pd.isna(windspeed):
                monthly_averages[month]["AverageWindSpeed"].append(windspeed)

            if not pd.isna(bulb_temp):
                if not isinstance(bulb_temp, int):
                    bulb_temp = (
                        bulb_temp[: len(bulb_temp) - 1]
                        if bulb_temp[-1] == "s"
                        else bulb_temp
                    )
                    monthly_averages[month]["AverageDryBulbTemperature"].append(
                        float(bulb_temp)
                    )

        for month in monthly_averages.keys():
            monthly_averages[month]["AverageWindSpeed"] = sum(
                monthly_averages[month]["AverageWindSpeed"]
            ) / len(monthly_averages[month]["AverageWindSpeed"])
            monthly_averages[month]["AverageDryBulbTemperature"] = sum(
                monthly_averages[month]["AverageDryBulbTemperature"]
            ) / len(monthly_averages[month]["AverageDryBulbTemperature"])

        # Write the monthly averages with Lat/Long
        return lat_lon, monthly_averages

    with beam.Pipeline() as pipeline:
        result = (
            pipeline
            | "Read text file"
            >> beam.Create(
                read_text_files("/root/airflow/logs/result/result.txt-00000-of-00001")
            )
            | "Compute avrages" >> beam.Map(compute_average)
            | "Write to text file" >> beam.io.WriteToText(output_file_path)
        )


def plot_images_task():

    def plot_images(line):
        month, data = eval(line.strip())
        df = pd.DataFrame(data)
        gdf = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4326"
        )
        world = gpd.read_file(get_path("naturalearth.land"))

        # Plot heatmaps for each field
        fields = ["AverageWindSpeed", "AverageDryBulbTemperature"]
        for field in fields:
            ax = world.plot(color="white", edgecolor="black")
            gdf.plot(ax=ax, column=field, cmap="viridis", legend=True)
            ax.set_title(f"{field} data for the month {month}")
            plt.savefig(f"{root_dir}images/{month}_{field}_heatmap.png")

    def format_averages_file():
        with open("/root/airflow/logs/result/averages.txt-00000-of-00001", "r") as file:
            data = [eval(line.strip()) for line in file]

        # Create a dictionary for each month
        months = {}
        for lat_lon, month_data in data:
            for month, values in month_data.items():
                if month not in months:
                    months[month] = {
                        "Latitude": [],
                        "Longitude": [],
                        "AverageWindSpeed": [],
                        "AverageDryBulbTemperature": [],
                    }
                months[month]["Latitude"].append(lat_lon[0])
                months[month]["Longitude"].append(lat_lon[1])
                months[month]["AverageWindSpeed"].append(values["AverageWindSpeed"])
                months[month]["AverageDryBulbTemperature"].append(
                    values["AverageDryBulbTemperature"]
                )
        with open("/root/airflow/logs/result/formatted_averages.txt", "w") as file:
            for month, data in months.items():
                file.write("(" + str((month, data)) + ")\n")

    format_averages_file()
    with beam.Pipeline() as pipeline:
        result = (
            pipeline
            | "Read text file"
            >> beam.Create(
                read_text_files("/root/airflow/logs/result/formatted_averages.txt")
            )
            | "Plot images" >> beam.Map(plot_images)
        )

def create_gif():
    # Create gifs using the png files generated before
    fps=12
    input_folder = "/opt/airflow/logs/png_archive/"
    output_folder = "/opt/airflow/logs/gif_archive/"

    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
    os.makedirs(output_folder)

    unique_identifier_name = [os.path.splitext(file)[0].split('_')[0] for file in os.listdir(input_folder) if file.endswith(".png")][0]

    for i in range(len(required_fields)):
        FIELD_NAME = required_fields[i]

        output_filename = unique_identifier_name + "_" + FIELD_NAME + ".gif"
        output_gif_path = os.path.join(output_folder, output_filename)

        # Extract all the png files having the same field name
        png_files = sorted([f for f in os.listdir(input_folder) if f.lower().endswith(FIELD_NAME.lower()+'.png')])
        
        input_files = [os.path.join(input_folder, file) for file in png_files]

        # Read PNG files and create GIF
        images = [imageio.imread(file) for file in input_files]
        imageio.mimsave(output_gif_path, images, duration=1000/fps, loop=3)

    return True
def clean_archive_folder():
    archive_dir = "/root/airflow/logs/archive/"

    # Delete the existing archive folder
    if os.path.exists(archive_dir):
        shutil.rmtree(archive_dir)
process_data_task = PythonOperator(
    task_id="process_data_task",
    python_callable=process_task,
    provide_context=True,
    dag=dag,
)

compute_monthly_average_task = PythonOperator(
    task_id="compute_monthly_average",
    python_callable=clean_archive_folder,
    provide_context=True,
    dag=dag,
)

plot_geomaps_task = PythonOperator(
    task_id="plot_heatmaps",
    python_callable=clean_archive_folder,
    provide_context=True,
    dag=dag,
)
create_gif_task = PythonOperator(
    task_id ='create_gif', 
    python_callable= create_gif, 
    provide_context = True,
    dag = dag)

clean_archive_task = PythonOperator(
    task_id='clean_archive_folder',
    python_callable=clean_archive_folder,
    provide_context=True,
    dag=dag
)

wait_for_archive >> unzip_files_task >> process_data_task >> compute_monthly_average_task >> plot_geomaps_task >> create_gif_task >>clean_archive_task