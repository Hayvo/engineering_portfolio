# Backend Service for am-gaap-event-journal-netsuite-tie-app

## Prerequisites

1. **Generate Access Key**:
   - Generate an access key for the `bigquery-admin@am-finance-forecast.iam.gserviceaccount.com` service account.
   - Rename the key file to `bigquery_service_account.json`.
   - Store the file in the `./var` directory.

## How to Run the App

1. **Install Dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

2. **Start the Application**:
   ```bash
   python app.py
   ```

## API Endpoints

### 1. `POST /login`

Authenticate the user if the email is from `adoreme.com` and return an access token.

**Header**

```json
{
  "Content-Type": "application/json"
}
```

**Request Body**:

```json
{
  "email": "user@adoreme.com",
  "email_verified": "true"
}
```

**Response**:

```json
{
  "status": "success",
  "access_token": "your_access_token",
  "message": "Login successful!"
}
```

### 2. `GET /api`

Return the Excel file in binary format for the corresponding journal and date range.

**Header**

```json
{
  "Authorization": "Bearer your_access_token"
}
```

**Request Parameters**:

- `journal`: The journal name.
- `date_from`: The start date.
- `date_to`: The end date.

**Response**:
Binary content of the Excel file.

### 3. `GET /journals`

Retrieve a list of all journals.

**Header**

```json
{
  "Authorization": "Bearer your_access_token"
}
```

**Response**:

```json
{
  "status": "success",
  "journals": [
    "journal1",
    "journal2",
    ...
  ]
}
```

## Notes

- Ensure that the `bigquery_service_account.json` file is correctly placed in the `./var` directory before starting the application.
- The application runs on port `5000` by default.
