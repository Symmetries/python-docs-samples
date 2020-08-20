
GCP_PROJECT=data-science-onramp
IMAGE_URI=gcr.io/$GCP_PROJECT/pytorch-model

export JOB_NAME=pytorch_training_job_gpu_$(date +%Y%m%d_%H%M%S)
export REGION=us-central1
export MODEL_DIR=pytorch_model_gpu_$(date +%Y%m%d_%H%M%S)
export BUCKET_NAME=citibikevd

gcloud ai-platform jobs submit training $JOB_NAME \
  --scale-tier BASIC_GPU \
  --region $REGION \
  --master-image-uri $IMAGE_URI \
  -- \
  --epochs=5 \
  --model-dir=gs://$BUCKET_NAME/$MODEL_DIR
