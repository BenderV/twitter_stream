## Twitter Stream

#### Installation
- `pyenv .venv`
- `source .venv/bin/activate`
- `pip3 install -r requirements.txt`

#### Create tables on DB
- `python3 models.py` 

#### Run Twitter Stream
- Add twitter oauth config on environement variable (see `twitter_stream.py`)
- Add your keywords "tracks" in `twitter_tracks.csv`
- `python3 twitter_stream.py`

