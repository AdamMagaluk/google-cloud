package co.cask.gcp.ml;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageSubmitterContext;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.gcp.common.GCPConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.Path;


/**
 * Google Cloud ML Transform which joins record with a prediction result.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(co.cask.gcp.ml.MLPredictTransform.NAME)
@Description(co.cask.gcp.ml.MLPredictTransform.DESCRIPTION)
public class MLPredictTransform extends Transform<StructuredRecord, StructuredRecord> {

  public static final String NAME = "SpeechToText";
  public static final String DESCRIPTION = "Converts audio files to text by applying powerful " +
      "neural network models.";

  private Config config;
  private Schema outputSchema = null;
  private MLPredictClientV1 mlClient;

  @SuppressWarnings("unused")
  public MLPredictTransform(Config config) {
    this.config = config;
  }
  
  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);
    config.validate(configurer.getStageConfigurer().getInputSchema());

    if (!config.containsMacro("schema")) {
      outputSchema = config.getSchema();
      configurer.getStageConfigurer().setOutputSchema(outputSchema);
    }

  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    config.validate(context.getInputSchema());
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    if (context.getInputSchema() != null) {
      outputSchema = getSchema(context.getInputSchema(), config.getOutputField());
    }

    mlClient = MLPredictClientV1
        .createWithConfig(getInstanceSchema(context.getInputSchema(), config.getInstancesField()),
            config);
  }

  /**
   * Request object from the Plugin REST call.
   */
  public static final class Request extends Config {

    private Schema inputSchema;
  }

  @Path("getSchema")
  public Schema getSchema(Request request) {
    return getSchema(request.inputSchema, request.getOutputField());
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) {
    // if an output schema is available then use it or else use the schema for the given input.
    Schema currentSchema;
    if (outputSchema != null) {
      currentSchema = outputSchema;
    } else {
      currentSchema = input.getSchema();
    }
    StructuredRecord.Builder outputBuilder = StructuredRecord.builder(currentSchema);
    emitter.emit(outputBuilder.build());
  }

  @Override
  public void destroy() {
    super.destroy();
    try {
      mlClient.shutdown();
    } catch (Exception e) {
      // no-op
    }
  }

  private Schema getSchema(Schema inputSchema, @Nullable String outputField) {
    List<Schema.Field> fields = new ArrayList<>();
    if (inputSchema.getFields() != null) {
      fields.addAll(inputSchema.getFields());
    }

    fields.add(Schema.Field.of((outputField != null) ? outputField : "prediction",
        Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));
    return Schema.recordOf("record", fields);
  }

  /*
      Returns a Schema based on the `inputSchema`, if `instancesField` is null all fields in the
      original inputSchema will be added to a Schema with `record`. If `instancesField` only the fields
   */
  private Schema getInstanceSchema(Schema inputSchema, @Nullable String instancesField) {
    List<Schema.Field> fields = new ArrayList<>();

    // If not provided use the entire input schema.
    if (instancesField == null) {
      fields.addAll(inputSchema.getFields());
    } else {
      if (inputSchema.getField(instancesField) == null) {
        throw new IllegalArgumentException("Either 'Instances Field' must be specified.");
      }
      fields.add(inputSchema.getField(instancesField));
    }

    return Schema.recordOf("record", fields);
  }

  private void copyFields(StructuredRecord input, StructuredRecord.Builder outputBuilder) {
    // copy other schema field values
    List<Schema.Field> fields = input.getSchema().getFields();
    if (fields != null) {
      for (Schema.Field field : fields) {
        outputBuilder.set(field.getName(), input.get(field.getName()));
      }
    }
  }

  /**
   *  Config for Cloud ML Predict Plugin.
   */
  protected static class Config extends GCPConfig {

    @Macro
    @Name("model")
    @Description("The name of model.")
    private String model;

    public String getModel() {
      if (containsMacro("model") || model == null ||
          model.isEmpty()) {
        return null;
      }
      return model;
    }

    @Macro
    @Name("modelVersion")
    @Description("The version of model.")
    private String modelVersion;

    public String getModelVersion() {
      if (containsMacro("modelversion") || modelVersion == null ||
          modelVersion.isEmpty()) {
        return null;
      }
      return modelVersion;
    }

    @Macro
    @Description("The schema of the table to read.")
    private String schema;

    @Macro
    @Name("instancesfield")
    @Nullable
    @Description("Name of field containing the instances to use in the prediction. When omitted" +
        "the entire input record is converted to JSON and passed.")
    private String instancesField;

    public String getInstancesField() {
      if (containsMacro("instancesfield") || instancesField == null ||
          instancesField.isEmpty()) {
        return null;
      }
      return instancesField;
    }

    @Macro
    @Name("outputfield")
    @Nullable
    @Description("The name of field to store the prediction results. Defaults to 'prediction'")
    private String outputField;

    public String getOutputField() {
      if (containsMacro("outputfield") || outputField == null ||
          outputField.isEmpty()) {
        return null;
      }
      return outputField;
    }

    @Macro
    @Name("predictionfield")
    @Nullable
    @Description("The field within the prediction instance to set to the output field. Defaults" +
        "to root of the returned prediction.")
    private String predictionField;

    public String getPredictionField() {
      if (containsMacro("predictionfield") || predictionField == null ||
          predictionField.isEmpty()) {
        return null;
      }
      return predictionField;
    }


    private boolean isValidInputType(Schema.Type type) {
      return true;
    }

    /**
     * @return the schema of the dataset
     * @throws IllegalArgumentException if the schema is null or invalid
     */
    public Schema getSchema() {
      if (schema == null) {
        throw new IllegalArgumentException("Schema must be specified.");
      }
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
      }
    }

    private void validate(@Nullable Schema inputSchema) {
      if (inputSchema != null) {
        // If a inputSchema and instancesfield is available then verify that the schema does contain
        // the given instancesfield and that the type is byte array. This will allow to fail fast
        // i.e. during deployment time.
        String instanceFieldName = getInstancesField();
        if (instanceFieldName != null && inputSchema.getField(instanceFieldName) == null) {
          throw new IllegalArgumentException(
              String.format("Field %s does not exists in the input schema.",
                  instanceFieldName));
        }
        Schema.Field field = inputSchema.getField(instanceFieldName);
        Schema fieldSchema = field.getSchema();
        Schema.Type fieldType =
            fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType()
                : fieldSchema.getType();

        if (!isValidInputType(fieldType)) {
          throw new IllegalArgumentException(
              String.format("Field '%s' is not a valid type but is of type " +
                  "'%s'.", field.getName(), fieldType));
        }

        Set<String> fields = inputSchema.getFields().stream().map(Schema.Field::getName).collect(
            Collectors.toSet());
        if (getOutputField() != null && fields.contains(getOutputField())) {
          throw new IllegalArgumentException(
              String.format("Input schema contains given Output Field " +
                  "'%s'. Please provide a different name.", getOutputField()));
        }
      }
    }
  }
}
