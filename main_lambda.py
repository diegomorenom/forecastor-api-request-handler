import json
import boto3
import pandas as pd
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import JSONResponse

app = FastAPI()

lambda_client = boto3.client('lambda')

@app.post("/process_forecast")
async def process_forecast(prediction_column: str = Form(...),
                           date_column: str = Form(...),
                           forecast_days: int = Form(...),
                           selected_models: str = Form(...),
                           csv_file: UploadFile = File(...)):
    # Read and save CSV file
    data = pd.read_csv(csv_file.file)
    data.columns = [date_column, prediction_column]
    data.columns = ['date', 'prediction_column']

    # Prepare payload for model Lambdas
    payload = {
        "data": data.to_csv(index=False),
        "forecast_days": forecast_days
    }

    selected_models_list = selected_models.split(',')

    results = {}
    for model in selected_models_list:
        response = lambda_client.invoke(
            FunctionName=f"forecastor_{model}_lambda",
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        result = json.loads(response['Payload'].read().decode('utf-8'))
        results[model] = result

    return JSONResponse(content=results)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)