package co.cask.gcp.ml;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.StructuredRecordStringConverter;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.concurrent.Future;

/**
 * Client for the GCP Cloud ML Predict API. Performs prediction on the data in the request.
 * loud ML Engine implements a custom predict verb on top of an HTTP POST method.
 */
public class MLPredictClientV1 {

  // https://cloud.google.com/ml-engine/reference/rest/v1/projects/predict
  // https://cloud.google.com/ml-engine/docs/v1/predict-request
  private static String baseURL = "https://ml.googleapis.com/v1/";
  private static String pathFormat = "%sprojects/%s/models/%s/versions/%s:predict";
  private static String instancesField = "instances";

  private HttpTransport httpTransport;
  private HttpRequestFactory requestFactory;
  private GenericUrl modelURL;
  private Schema schema;

  /*
    Initialize a MLClient using the MLPredictPlugin.Config
   */
  public static MLPredictClientV1 createWithConfig(Schema recordSchema,
      MLPredictTransform.Config config)
      throws IOException, GeneralSecurityException {
    return new MLPredictClientV1(recordSchema, config.getServiceAccountFilePath(),
        config.getProject(),
        config.getModel(),
        config.getModelVersion());
  }


  public MLPredictClientV1(Schema recordSchema, String serviceAccountFile, String projectId,
      String modelId,
      String versionId)
      throws IOException, GeneralSecurityException {
    this.httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    this.schema = Schema.recordOf("apiformat",
        Schema.Field.of(instancesField, Schema.arrayOf(recordSchema))
    );

    GoogleCredential credential;
    if (serviceAccountFile != null) {
      InputStream inputStream = new FileInputStream(serviceAccountFile);
      credential = GoogleCredential.fromStream(inputStream);
    } else {
      credential = GoogleCredential.getApplicationDefault();
    }

    requestFactory = httpTransport.createRequestFactory(credential);
    modelURL = new GenericUrl(String.format(pathFormat, baseURL, projectId, modelId, versionId));
  }

  public Future<HttpResponse> predict(StructuredRecord input) throws IOException {
    HttpContent content = convertToContent(input);
    HttpRequest request = requestFactory.buildPostRequest(modelURL, content);
    return request.executeAsync();
  }

  /*
    Calls shutdown() on the HTTP Transport which by default does nothing.
  */
  public void shutdown() throws IOException {
    httpTransport.shutdown();
  }

  /*
    Converts a StructuredRecord to a JSON string based on configured input schema. If for example
    the input schema was defined as `{ "age": INT, "city": STRING}` and the given input as:
     `{ "age": 29, "city": "Detroit"}`.
    The resulting JSON content for the API would be:
     `{"instances": [{ "age": 29, "city": "Detroit"}]}`

    Note: Only a single instance is used in the API request. More details on the predict request
    details: https://cloud.google.com/ml-engine/docs/v1/predict-request
   */
  protected HttpContent convertToContent(StructuredRecord input) throws IOException {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    StructuredRecord[] records = {input};
    builder.set(instancesField, records);

    String body = StructuredRecordStringConverter.toJsonString(builder.build());
    return new ByteArrayContent("application/json", body.getBytes());
  }

  public String getModelURL() {
    return modelURL.toString();
  }
}
