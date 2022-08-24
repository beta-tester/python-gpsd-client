# python-gpsd-client
a gpsd-client in python is using python threading.

this is a test project only. use it at your own risk.

## threads:
- thread `WorkerDatabase` is collecting data from a `queue` and store the data to a sqlite database.
- thread `WorkerScript` gets triggered to execute a script to do some stuff with the data in the sqlite database.
- thread `WorkerGps` is receiving gps data from a gpsd-server and put the data to a `queue` for thread `WorkerDatabase`.

in this example there are two `WorkerGps` threads to collect data from two different gpsd-servers (`HOST_1` and `HOST_2`).

## sqlite database content:
- table `data_gps_tpv` stores position data like: `lat`, `lon`, `alt`, ...
- table `data_gps_sky` stores satellite data: `el`, `az`, ...
- table `data_gps_sky_map` stores the strength of the satellites data accorting to their position on the sky (makes only sense when the gps device is at a fix position)

## path:
the output path for the sqlite file is the temporary folder (e.g.: /tmp/), but can be changed in `OUTPUT_PATH`.
the output path for the logging file is the temporary folder (e.g.: /tmp/), but can be changed in `LOGGER_OUTPUT_PATH`.

## features:
- in case the gpsd-client isn't receiving data from the gpsd-server anymore for a certain number of seconds, a restart of the `WorkerGps` thread will be triggered.
  1. when gps-client receives no data at all for longer than `TIMEOUT_STOP_WORKER_GPS`.
  2. when gps-client returns no valid data for longer thant `TIMEOUT_STOP_WORKER_GPS`.
  3. when gps-client returns same data over and over for longer than `TIMEOUT_STOP_WORKER_GPS`.
  
  it gives up and stops itself to get restarted.
  these three cases happens to me because i have a very weak wifi connection to my gpsd-server. and all the above cases i have to handle to get a reliable client.
  
- if the `queue` isn't filled up with data for a certain number of seconds (`TIMEOUT_STOP_WORKER_DATABASE`) the whole application will stop.

- the collected data will be stored to an in-memory sqlite database first and after a certain number of seconds (`COMMIT_INTERVAL`) the data will be stored to the database file, to reduce the disk I/O (wearout).

- at each commit to the database file an optional script will be triggered by signaling the `WorkerScript` thread.</br>in my case a `gnuplot` script (not included):</br>
  ![plot_gps](https://user-images.githubusercontent.com/4750719/186476010-fa946047-6f9e-42da-87f7-65337ef11c98.jpg)

- logging debug information to a separate file

## requirements:

- `python` 3.9 or newer
- APT package: `gpsd-client` 3.22 or newer. or compiled from newest [source code](https://gitlab.com/gpsd/gpsd.git)
- APT package: `sqlite3`
- APT package: `gnuplot` (optional)

## see also:
- my project [RPi-GPS-PPS-StratumOne](https://github.com/beta-tester/RPi-GPS-PPS-StratumOne) is setting up a gpsd-server you can use to collect gps data from.
