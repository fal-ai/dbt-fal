# Docker for fal
Build local fal Docker images

## Building an image
In order to build a new fal image, run the following docker command:
```
docker build <path/to/dockerfile>
```
You can also pull it from the DockerHub:
```
docker pull falai/fal
```
## Inside your docker-compose.yml
You can incorporate the fal docker image in your docker-compose.yml:

```yaml
...
services:
...
    fal-service:
        image: falai/fal:latest
```
