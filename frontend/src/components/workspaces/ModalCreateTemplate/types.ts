import type { CommandPolicy, EnvironmentType } from '../../../generated-types';

export type Interval = {
  max: number;
  min: number;
};

export type Resources = {
  cpu: Interval;
  disk: Interval;
  ram: Interval;
};

export type TemplateForm = {
  name: string;
  description: string;
  environments: TemplateFormEnv[];
  cleanup?: {
    deleteAfterCreation?: string;
    stopAfterInactivity?: string;
    deleteAfterInactivity?: string;
  };
  allowPublicExposure: boolean;
  nodeSelector?: Record<string, string> | null;
  // The Jobs section; absent where the section is not shown.
  job?: TemplateFormJob;
  // On edit, spec.job as it was read: the parts the form does not show are
  // kept from it, and the patch is computed against it.
  savedJob?: TemplateJobSpec | null;
};

// spec.job, as the API holds it.
export type TemplateJobSpec = {
  environmentName: string;
  commandPolicy: CommandPolicy;
  command?: string[];
  maxRunTime?: string;
  otherResources?: Record<string, string>;
};

export type TemplateFormJob = {
  enabled: boolean;
  environmentName?: string;
  commandPolicy: CommandPolicy;
  // The script run with sh -c.
  commandText?: string;
  maxRunTime?: string;
  // By the keys of the environment's widget (e.g., nvidiaComGpu).
  otherResources?: Record<string, number | null | undefined>;
};

export type TemplateFormEnv = {
  name: string;
  environmentType: EnvironmentType;
  image: string;
  registry: string;
  gui: boolean;
  cpu: number;
  ram: number;
  persistent: boolean;
  reservedCpu: number;
  disk: number;
  sharedVolumeMounts: TemplateFormEnvShVol[];
  rewriteUrl?: boolean;
  disableControls?: boolean;
  containerStartupOptions?: ContainerStartupOptionsForm;
  storageClassName?: string;
  mountMyDriveVolume?: boolean;
  otherResources?: Record<string, number>;
};

export type ContainerStartupOptionsForm = {
  sourceArchive: string;
  contentPath: string;
  startupArgs: string[];
  EnforceWorkDir: boolean;
};

export type ChildFormItem = {
  parentFormName: number;
  restField: {
    fieldKey?: number | undefined;
  };
};

export interface TemplateFormEnvShVol {
  sharedVolume: string; // workspace/shvol
  mountPath: string;
  readOnly: boolean;
}
export type Image = {
  name: string;
  type: Array<EnvironmentType>;
  registry: string;
};

export type ImageList = {
  projectBaseName?: string;
  name: string;
  registryName: string;
  images: Array<{
    name: string;
    versions: Array<string>;
    versionDetails?: Array<{
      version: string;
      volumeSize?: string;
    }>;
  }>;
};
