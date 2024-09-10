from datetime import datetime
import json
from typing import Any, Dict, Optional
from fastapi import Body, FastAPI, File, Query, Response, UploadFile, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
import pandas as pd
import aiofiles
import io
from io import BytesIO
from bson import json_util
from pymongo import MongoClient, UpdateOne
import uuid  # Import UUID
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

# MongoDB connection
client = AsyncIOMotorClient("mongodb+srv://aryank013:prajit@kv-consult.vsqin.mongodb.net/?retryWrites=true&w=majority&appName=kv-consult")
db = client["kv"]
collection = db["master"]

async def process_excel_file(file: UploadFile):
    # Read the file content into memory
    contents = await file.read()

    # Use Pandas to read the Excel file
    excel_data = pd.read_excel(io.BytesIO(contents))

    # Convert the DataFrame to a list of dictionaries
    data = excel_data.to_dict(orient="records")
   

    # Convert keys to lowercase and replace spaces with underscores
    processed_data = []
    for record in data:
        processed_record = {key.lower().replace(" ", "_"): value for key, value in record.items()}
        processed_record["inserted_at"] = datetime.utcnow()  # Add the current date and time
        processed_data.append(processed_record)

    # Prepare the bulk write operations
    operations = []
    for record in processed_data:
        name_email_filter = {"name": record.get("name"), "email_id": record.get("email_id")}
        email_phone_filter = {
        "email_id": record.get("email_id"),
        # Only include phone number in the filter if it is not None
        **({"phone_number": record.get("phone_number")} if record.get("phone_number") is not None else {})
    }
       


        # Check if either combination already exists
        existing_record = await collection.find_one({"$or": [name_email_filter, email_phone_filter]})
        print(existing_record)

        if existing_record:
            # If a record exists, overwrite it
            operations.append(UpdateOne(
                {"_id": existing_record["_id"]},
                {"$set": record}
            ))
        else:
            record["_id"] = uuid.uuid4().hex
            operations.append(UpdateOne(
                {"_id": record["_id"]},  # Use the generated UUID
                {"$set": record},
                upsert=True
            ))
    print(operations)
    if operations:
        await collection.bulk_write(operations)
    else:
        raise HTTPException(status_code=400, detail="No data found in the Excel file.")

@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    # Check file type
    if file.content_type != 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    # Process the file and load data into MongoDB
    await process_excel_file(file)
    return {"message": "Data uploaded successfully!"}

async def update_mongodb_records(file: UploadFile):
    # Load the Excel file with all sheets
    xls = pd.ExcelFile(file.file)
    
    # Iterate through each sheet in the Excel file
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        
        # Process each row in the DataFrame
        for _, row in df.iterrows():
            # Define the query to check if at least two of the three fields match
            query = {
                "$or": [
                    {"$and": [{"name": row.get("name")}, {"phone_number": row.get("phone_number")}]},
                    {"$and": [{"name": row.get("name")}, {"email": row.get("email")}]},
                    {"$and": [{"phone_number": row.get("phone_number")}, {"email": row.get("email")}]}
                ]
            }
            
            # Search for the existing record with at least two matching fields
            existing_record = collection.find_one(query)
            
            if existing_record:
                # Update the existing record with the new data
                update = {"$set": row.to_dict()}
                collection.update_one(query, update)
            # If no matching record is found, skip it (no insertion)

@app.get("/export/")
async def export_to_excel():
    # Fetch all records from MongoDB asynchronously
    cursor = collection.find({})
    records = await cursor.to_list(length=None)  # Convert cursor to list
    
    # Convert MongoDB records to a DataFrame
    df = pd.DataFrame(records)

    # Drop the MongoDB internal ID field if not needed
    if '_id' in df.columns:
        df = df.drop('_id', axis=1)

    # Create an in-memory Excel file
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    output.seek(0)

    # Send the Excel file as a response
    headers = {
        'Content-Disposition': 'attachment; filename="mongodb_data.xlsx"',
        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    }
    return Response(content=output.getvalue(), headers=headers)


@app.post("/filter-records/")
def filter_records(filters: Dict[str, Any] = Body(...)):
    client = MongoClient("mongodb+srv://aryank013:prajit@kv-consult.vsqin.mongodb.net/?retryWrites=true&w=majority&appName=kv-consult")
    db = client["kv"]
    collection = db["master"]   
    # Construct the MongoDB query from the provided filters
    query = {}

    for key, value in filters.items():
        query[key] = value

    # Execute the query
    records = list(collection.find(query))  # Limit to 100 records for example

    if not records:
        raise HTTPException(status_code=404, detail="No records found matching the filters.")

    # Return the records as JSON
    return json.loads(json_util.dumps(records))