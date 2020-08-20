
GCP_PROJECT=data-science-onramp
IMAGE_URI=gcr.io/$GCP_PROJECT/pytorch-model

export MODEL_DIR=pytorch_model_local_$(date +%Y%m%d_%H%M%S)
export BUCKET_NAME=citibikevd

docker run \
-e GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/key.json \
-v $GOOGLE_APPLICATION_CREDENTIALS:/tmp/keys/key.json:ro \
$IMAGE_URI --epochs=1 --model-dir=gs://$BUCKET_NAME/$MODEL_DIR
