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
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;

import org.junit.Test;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Tests for MLClient
 */
public class MLTest {

  @Test
  public void convertSchemaTest() throws Exception {
    String projectId = "adammagaluk-iot";
    String modelId = "census";
    String versionId = "v1";
    String apiBaseURL = "https://ml.googleapis.com/v1/";

    Logger logger = Logger.getLogger(HttpTransport.class.getName());
    logger.setLevel(Level.CONFIG);

    System.setProperty("com.google.api.client.http.level", "CONFIG");


    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();


    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

    String path = String.format(
        "%sprojects/%s/models/%s/versions/%s:predict", apiBaseURL, projectId, modelId, versionId);

    GenericUrl url = new GenericUrl(path);
    String jsonString = "{\"instances\": [{\"age\": 25, \"capital_gain\": 0, \"capital_loss\": 0," +
    "\"education\": \" 11th\", \"education_num\": 7, \"gender\": \" Male\"," +
    "\"hours_per_week\": 40, \"marital_status\": \" Never-married\", " +
    "\"native_country\": \" United-States\", \"occupation\": \" Machine-op-inspct\"," +
    "\"race\": \" White\", \"relationship\": \" Own-child\", \"workclass\": \" Private\"}]}";
    HttpContent content = new ByteArrayContent("application/json", jsonString.getBytes());

    GoogleCredential credential = GoogleCredential.getApplicationDefault();
    HttpRequestFactory requestFactory = httpTransport.createRequestFactory(credential);

    for (int i = 0; i < 100; i++) {
      HttpRequest request = requestFactory.buildPostRequest(url, content);
      String response = request.execute().parseAsString();
      System.out.println(response);
    }
  }

  @Test
  public void convertSchemaTestAsync() throws Exception {
    String projectId = "adammagaluk-iot";
    String modelId = "census";
    String versionId = "v1";
    String apiBaseURL = "https://ml.googleapis.com/v1/";

    Logger logger = Logger.getLogger(HttpTransport.class.getName());
    logger.setLevel(Level.CONFIG);

    System.setProperty("com.google.api.client.http.level", "CONFIG");


    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();


    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

    String path = String.format(
        "%sprojects/%s/models/%s/versions/%s:predict", apiBaseURL, projectId, modelId, versionId);

    GenericUrl url = new GenericUrl(path);
    String jsonString = "{\"instances\": [{\"age\": 25, \"capital_gain\": 0, \"capital_loss\": 0," +
        "\"education\": \" 11th\", \"education_num\": 7, \"gender\": \" Male\"," +
        "\"hours_per_week\": 40, \"marital_status\": \" Never-married\", " +
        "\"native_country\": \" United-States\", \"occupation\": \" Machine-op-inspct\"," +
        "\"race\": \" White\", \"relationship\": \" Own-child\", \"workclass\": \" Private\"}]}";
    HttpContent content = new ByteArrayContent("application/json", jsonString.getBytes());

    GoogleCredential credential = GoogleCredential.getApplicationDefault();
    HttpRequestFactory requestFactory = httpTransport.createRequestFactory(credential);

    Set<Future<HttpResponse>> futures = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      HttpRequest request = requestFactory.buildPostRequest(url, content);
      futures.add(request.executeAsync());
    }

    for (Future<HttpResponse> future : futures) {
      String response = future.get().parseAsString();
      System.out.println(response);
    }
  }

  @Test
  public void covertSchemaToJSON() throws Exception {
    Schema record =
        Schema.recordOf("record",
            Schema.Field.of("transcript", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("confidence", Schema.of(Schema.Type.DOUBLE))
        );

    Schema apiFormat =
        Schema.recordOf("record2",
            Schema.Field.of("instances", Schema.arrayOf(record))
        );

    StructuredRecord.Builder outputBuilder = StructuredRecord.builder(record);
    outputBuilder.set("transcript", "test");
    outputBuilder.set("confidence", 21.2);

    StructuredRecord.Builder outputBuilderAPI = StructuredRecord.builder(apiFormat);
    StructuredRecord[] records = {outputBuilder.build()};
    outputBuilderAPI.set("instances", records);

    String body = StructuredRecordStringConverter.toJsonString(outputBuilderAPI.build());

  }

}
