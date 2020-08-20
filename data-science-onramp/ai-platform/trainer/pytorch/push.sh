
GCP_PROJECT=data-science-onramp
IMAGE_URI=gcr.io/$GCP_PROJECT/pytorch-model

docker push $IMAGE_URI
