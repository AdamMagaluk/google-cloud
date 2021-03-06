/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.gcp.gcs.actions;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.gcp.gcs.StorageClient;

import java.io.IOException;
import javax.annotation.Nullable;


/**
 * An action plugin to move GCS objects.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(GCSMove.NAME)
@Description("Moves objects in Google Cloud Storage.")
public class GCSMove extends Action {
  public static final String NAME = "GCSMove";
  private Config config;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
  }

  @Override
  public void run(ActionContext context) throws IOException {
    config.validate();
    StorageClient storageClient = StorageClient.create(config.getProject(), config.getServiceAccountFilePath());
    //noinspection ConstantConditions
    storageClient.move(config.getSourcePath(), config.getDestPath(), config.recursive, config.shouldOverwrite());
  }

  /**
   * Config for the plugin.
   */
  public static class Config extends SourceDestConfig {

    @Macro
    @Nullable
    @Description("Whether to move objects in all subdirectories.")
    private Boolean recursive;

    public Config() {
      super();
      recursive = false;
    }
  }
}
