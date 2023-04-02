[ -n "$(docker images -q tabular-comp:latest)" ] || sudo docker build -t tabular-comp .
[ -z "$(docker images -q tabular-comp:latest)" ] || echo "Container already built."
python3 run.py
#sudo docker run --name tabular-comp tabular-comp