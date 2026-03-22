## How to prepare local environment for testing

### 1) Install dockers
```
./test_kafka_sf/build_dockers/docker compose up -d
```

### 2) Create RSA Key Pair Authentication for Snowflake
- This is required for Kafka Sink Connector to be able to connect to Snowflake  
- The connector uses the private key; Snowflake stores the public key.  

#### Step 1: Generate RSA Private Key (PEM format, a text-based (base64-encoded)  
- ` ./snowflake_creds/openssl genrsa -out snowflake_key.pem 2048`
#### Step 2: Convert Private Key to PKCS#8 DER Format 
- **Snowflake Connector expects an unencypted (or encrypted with a passpharse) Private key in the DER formant**  
  `./snowflake_creds/openssl pkcs8 -topk8 -inform PEM -outform DER -in snowflake_key.pem -out snowflake_key.p8 -nocrypt`
### Step 3: Base64 Encode the DER Key
- **Snowlake Sink Connector reuries Private Key to be one liner. Base64-encodes the binary DER file into a single-line string  
The Private Key will be used as a value for the `snowflake.private.key` parameter.  
**On macOS:** `./snowflake_creds/base64 -i snowflake_key.p8 > snowflake_key.b64`  
**On Linux/WSL2:** `./snowflake_creds/base64 snowflake_key.p8 > snowflake_key.b64`

### Step 4: Extract Public Key for Snowflake  
- **Note:** Use the original PEM file (not the DER file) to extract the public key  
- The public key in PEM format (which Snowflake expects)  
`./snowflake_creds/openssl rsa -in snowflake_key.pem -pubout -out snowflake_key.pub`

### 3) Set Public Key for Snowflake user
```sql
ALTER USER <USER_NAME> 
SET RSA_PUBLIC_KEY='<snowlake_key.pub key here , no cr no new lines>;
```

### 4) Create venv and install required libraries
- `test_kafka_sf % python3 -m venv .venv`
- `source .venv/bin/activate`
-`(.venv)  test_kafka_sf % pip install -r requirements_test.txt `
