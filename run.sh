[ -n "$(docker images -q tabular-comp:latest)" ] || sudo docker build -t tabular-comp .
[ -z "$(docker images -q tabular-comp:latest)" ] || echo "Container already built."
#Set up virtual environment
pip3 install virtualenv
mkdir polarsPandasSpark-venv
#cd polarsPandasSpark-venv
python3 -m venv polarsPandasSpark-venv/env
pip3 install -r requirements-venv.txt
python3 run.py
#sudo docker run --name tabular-comp tabular-comp