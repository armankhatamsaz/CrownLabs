import gql from 'graphql-tag';

export default gql`
  subscription updatedWorkspaceTemplates(
    $workspaceNamespace: String!
    $templateId: String
  ) {
    updatedTemplate: itPolitoCrownlabsV1alpha2TemplateUpdate(
      namespace: $workspaceNamespace
      name: $templateId
    ) {
      updateType
      template: payload {
        spec {
          prettyName
          description
          allowPublicExposure
          job {
            environmentName
            commandPolicy
            command
            maxRunTime
            otherResources
          }
          environmentList {
            name
            guiEnabled
            persistent
            resources {
              cpu
              disk
              memory
              reservedCPUPercentage
              otherResources
            }
          }
          workspaceCrownlabsPolitoItWorkspaceRef {
            name
          }
        }
        metadata {
          name
          namespace
        }
      }
    }
  }
`;
