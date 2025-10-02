This code builds three docker containers
- postgres database for storing hot data (<= 4 weeks) of MUNI vehicle positions
- fastapi as middle between database and front end
- fetcher to continuously push vehicle records to the postgres db, also initializes scripts to export old partitions to S3 and drop them from the postgres db

All of this is being run on a t2.micro with an api access point at http://184.72.9.93:8000/vehicles/current .

I will be editing this README soon so it is more informative and walks through set-up more clearly.

The map can be found at https://polar3197.github.io/map/ .
