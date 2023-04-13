# polars-pandas-spark

<!--
*** Written by Martin Karlsson
*** www.martinkarlsson.io
-->

[![LinkedIn][linkedin-shield]][linkedin-url]


<!-- ABOUT THE PROJECT -->
## About The Project

Comparison between three popular Python packages for handling tabular data.
The test starts 4 x 3 containers in series and logs the CPU, Memory and Time consumed for each.
Pandas, Spark and Polars will be used to find unique values on a given column, and in an iteration grouping and sum up the column values.

Spark:

(`testDf.select("id").rdd.flatMap(lambda x: x).collect()`)

(`testDf.groupby("id").sum()`)


Pandas:

(`list(testDf['id'].unique())`)

(`testDf.groupby("id").sum()`)


Polars:

(`list(testDf['id'].unique())`)

(`testDf.groupby("id").sum()`)


![Architecture overview][arch]

## Pre-requisite
- Confirmed working on Linux (and WSL on Windows)
- Ensure Docker is installed.

## Start

Execute `bash run.sh` to start the the build and start the containers.

<!-- CONTRIBUTING -->
## Contributing

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/featureName`)
3. Commit your Changes (`git commit -m 'Add some featureName'`)
4. Push to the Branch (`git push origin feature/featureName`)
5. Open a Pull Request


<!-- CONTACT -->
## Contact

### Martin Karlsson

LinkedIn : [martin-karlsson][linkedin-url] \
Twitter : [@HelloKarlsson](https://twitter.com/HelloKarlsson) \
Email : hello@martinkarlsson.io \
Webpage : [www.martinkarlsson.io](https://www.martinkarlsson.io)


Project Link: [github.com/martinkarlssonio/big-data-solution](https://github.com/martinkarlssonio/big-data-solution)


<!-- MARKDOWN LINKS & IMAGES -->
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/in/martin-karlsson
[arch]: arch.png