# Google Cloud ML Predict Transform

Description
-----------
This uses Cloud ML's [Predict API](https://cloud.google.com/ml-engine/reference/rest/v1/projects/predict) to
join the input schema with the prediction results.

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be
provided and can be set to 'auto-detect'.
Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Cloud Platform Console.
Make sure the account key has permission to access Google Cloud Spanner.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

Properties
----------
**Model:** The Cloud ML model to run the prediction against.

**Model Version:** The Cloud ML model version to run the prediction against.

**Instances Field:** The input field to be converted to JSON and sent as a prediction instance to Cloud ML. If this field is left
empty the plugin will convert the entire input record to JSON and use it.

**Output Field**: The field to store the prediction result. Defaults to `prediction`.

**Prediction Field**: A sub field to extract from the prediction returned to convert to the output schema of the **Output Field**. If
left empty the root of the prediction result is returned.

