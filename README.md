# Starcloud Downloader Tool
## Setup
Most easy python installation you could imagine.
It's as easy as running:

```sh
uv sync
```

or

```sh
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```

This created and activated a pip environment and installed all needed packages to run this lovely script.

Then you need to enter your *Starcloud* credentials in the `.env` file.
For this, please create a copy of the already existing `example.env` file:
```sh
cp example.env .env
```

**!!! The credentials are important in order to fetch the tile files**

You need to get them from your browser after logging in!
To do this, follow these steps:
1. Go to this page `https://data-starcloud.pcl.ac.cn/iearthdata/login`
2. Open the browser development console (in firefox press \<F12>)
3. Go to the network tab
4. Back to the website, now enter your credentials and press the 'Login' button
5. Look at the requests that happened and find a *POST* request named 'authenticate' (you can also use the search bar)
6. Click on this request and then click on the 'Response' tab to see the server response to your request.
7. You should see a JSON object that looks like this:
```json
{
  "success": true,
  "failCode": null,
  "failReason": null,
  "data": {
    "id": 0000, // your user-ID
    "userName": "YOUR_USER_NAME", // your user name
    "password": null,
    "email": "EMAIL@EMAIL.com", // your E-Mail
    "code": null,
    "token": "eyJhbGci[.....]", // your JWT token -> starts with 'ey...'
    "functionNameList": null,
    "userId": "0000" // again your user-ID
  }
}
```
8. Copy the username, the token and your user Id and enter them into the `.env` file accordingly.
9. DONE

## How to use it
It is as simple as running:
```sh
python3 starcloud_dl.py TILE_NUMBER
```
The tool then downloads all tile from 2000-2022 (the years can be configured as parameters).

**Attention:** The token you extracted from your browser is only valid for one hour (1h)!
Meaning that if you run this script and the downloading takes longer than one hour, you will get download errors! However, you can easily repeat the previous steps to get the token (username and id stay the same). And then re-run the script.
The script has an index feature to prevent re-downloading already downloaded files.

Depending on your bandwith you can run multiple instances of this script to download multiple tiles concurrently.
If you change the output dir with the `-o` parameter you need to pay attention that all running instances of this script write to the same location.

A simple way to download multiple tiles at the same time would be i.e.:
```sh
cat tiles.txt | xargs -n1 -P 4 python3 starcloud_dl.py --no-progress
```
Assuming that `tiles.txt` is a simple text file containing multiple tile names in the following format:
```txt
32UQB
32UQC
32UQD
32UQE
```
These would be piped into the script and the script would then start 4 times with different tile names, enabling concurrent downloading of multiple tiles.
When running multiple downloads at the same time, it is recommended to decrease the chunk size a little bit - but try out what works best.

## Parameters
```sh
usage: StarCloud Downloader [-h] [-e ENV_FILE] [--start-year START_YEAR] [--end-year END_YEAR] [-o OUTPUT_DIR] [-c CHUNK_SIZE] [--no-progress] tile

Lets you download all tiles for a range of years. Login credentials need to be passed by the '.env' file.

positional arguments:
  tile                  Tile that should be downloaded. i.e. '31UFS'

options:
  -h, --help            show this help message and exit
  -e, --env-file ENV_FILE
                        Filepath of env file that is used for authentication (default: .env)
  --start-year START_YEAR
                        Tile download starting year (default: 2000)
  --end-year END_YEAR   Tile download ending year (default: 2022)
  -o, --output-dir OUTPUT_DIR
                        Directory to which the files should be written to (default: ./)
  -c, --chunk-size CHUNK_SIZE
                        Sets the download chunk size in bytes. 1048576 bytes (aka. 1Mb) is recommended for most use cases, but when running multiple instances of this script a smaller chunk size can be
                        beneficial. (default: 1048576)
  --no-progress         Flag to disable progress output when downloading files. This is useful when running multiple instances of this script in parallel. (default: True)
```