import { ApolloError } from '@apollo/client';
import { CommandPolicy, EnvironmentType } from '../../../generated-types';
import {
  apiErrorTexts,
  effectiveRunLimit,
  MAX_RUN_TIME_PATTERN,
} from '../../activePage/TableInstance/RunJob/crownJobUtils';
import type {
  TemplateFormEnv,
  TemplateFormJob,
  TemplateJobSpec,
} from './types';

// The pattern of job.environmentName in the CRD, the same as an environment's.
export const JOB_ENVIRONMENT_NAME_PATTERN = /^[a-z\d][a-z\d-]{2,10}[a-z\d]$/;

// The extended resources offered for jobs: the keys and the bound of the
// environment's widget (Environment.tsx), with their Kubernetes names.
export const JOB_RESOURCE_FIELDS = [
  { key: 'nvidiaComGpu', name: 'nvidia.com/gpu', label: 'NVIDIA GPU' },
  { key: 'amdComGpu', name: 'amd.com/gpu', label: 'AMD GPU' },
] as const;
export const JOB_RESOURCE_MAX = 4;

// spec.job as the GraphQL queries return it.
export type TemplateJobData = {
  environmentName: string;
  commandPolicy?: CommandPolicy | null;
  command?: ReadonlyArray<string | null> | null;
  maxRunTime?: string | null;
  otherResources?: Record<string, unknown> | null;
};

// spec.job as read, with the CRD's default policy and no empty fields.
export const templateJobSpec = (
  job?: TemplateJobData | null,
): TemplateJobSpec | null => {
  if (!job) return null;
  const command = (job.command ?? []).filter(
    (arg): arg is string => arg !== null,
  );
  const otherResources = Object.fromEntries(
    Object.entries(job.otherResources ?? {}).map(([name, value]) => [
      name,
      String(value),
    ]),
  );
  return {
    environmentName: job.environmentName,
    commandPolicy: job.commandPolicy ?? CommandPolicy.Editable,
    ...(command.length > 0 && { command }),
    ...(job.maxRunTime && { maxRunTime: job.maxRunTime }),
    ...(Object.keys(otherResources).length > 0 && { otherResources }),
  };
};

// The script of a command of the form the editor writes: sh -c <script>.
export const shScript = (command?: readonly string[]): string | undefined =>
  command?.length === 3 && command[0] === 'sh' && command[1] === '-c'
    ? command[2]
    : undefined;

// A command of another form is shown as its arguments, and kept unless a new
// one is typed.
export const keptCommand = (
  saved?: TemplateJobSpec | null,
): string[] | undefined =>
  saved?.command?.length && shScript(saved.command) === undefined
    ? saved.command
    : undefined;

// The resources the widget shows (whole numbers up to its bound), and those it
// does not, which are kept as they are.
export const splitJobResources = (
  resources: Record<string, string> = {},
): { widget: Record<string, number>; kept: Record<string, string> } => {
  const widget: Record<string, number> = {};
  const kept: Record<string, string> = {};
  for (const [name, value] of Object.entries(resources)) {
    const field = JOB_RESOURCE_FIELDS.find(f => f.name === name);
    if (field && /^\d+$/.test(value) && Number(value) <= JOB_RESOURCE_MAX) {
      widget[field.key] = Number(value);
    } else {
      kept[name] = value;
    }
  }
  return { widget, kept };
};

export const templateFormJob = (
  saved?: TemplateJobSpec | null,
): TemplateFormJob => {
  if (!saved) return { enabled: false, commandPolicy: CommandPolicy.Editable };
  return {
    enabled: true,
    environmentName: saved.environmentName,
    commandPolicy: saved.commandPolicy,
    commandText: shScript(saved.command) ?? '',
    maxRunTime: saved.maxRunTime ?? '',
    otherResources: splitJobResources(saved.otherResources).widget,
  };
};

// spec.job from the form: undefined leaves it as it is (the section is not
// shown), null removes it.
export const templateJobFromForm = (
  job: TemplateFormJob | undefined,
  saved?: TemplateJobSpec | null,
): TemplateJobSpec | null | undefined => {
  if (!job) return undefined;
  if (!job.enabled) return null;
  const script = job.commandText?.trim();
  const command =
    job.commandPolicy === CommandPolicy.ImageDefault
      ? undefined
      : script
        ? ['sh', '-c', script]
        : keptCommand(saved);
  const otherResources = { ...splitJobResources(saved?.otherResources).kept };
  for (const { key, name } of JOB_RESOURCE_FIELDS) {
    const value = job.otherResources?.[key];
    if (value) otherResources[name] = String(value);
  }
  const maxRunTime = job.maxRunTime?.trim();
  return {
    environmentName: job.environmentName ?? '',
    commandPolicy: job.commandPolicy,
    ...(command && { command }),
    ...(maxRunTime && { maxRunTime }),
    ...(Object.keys(otherResources).length > 0 && { otherResources }),
  };
};

export type JsonPatchOperation =
  | { op: 'add' | 'replace'; path: string; value: unknown }
  | { op: 'remove'; path: string };

const canonical = (value: unknown): string =>
  JSON.stringify(value, (_, v) =>
    v && typeof v === 'object' && !Array.isArray(v)
      ? Object.fromEntries(
          Object.entries(v).sort(([a], [b]) => a.localeCompare(b)),
        )
      : v,
  );

// The patch of spec.job: add when jobs are turned on, replace when they are
// changed, remove when they are turned off, nothing otherwise.
export const templateJobPatch = (
  saved: TemplateJobSpec | null | undefined,
  next: TemplateJobSpec | null | undefined,
): JsonPatchOperation[] => {
  if (next === undefined) return [];
  if (next === null) return saved ? [{ op: 'remove', path: '/spec/job' }] : [];
  if (!saved) return [{ op: 'add', path: '/spec/job', value: next }];
  if (canonical(saved) === canonical(next)) return [];
  return [{ op: 'replace', path: '/spec/job', value: next }];
};

const NOT_A_CONTAINER: Partial<Record<EnvironmentType, string>> = {
  [EnvironmentType.VirtualMachine]: 'a virtual machine',
  [EnvironmentType.CloudVm]: 'a cloud VM',
  [EnvironmentType.LocalVm]: 'a local VM',
};

// Why an environment cannot run jobs (the Template's CEL rule), or null.
export const jobEnvironmentProblem = (
  env: Pick<TemplateFormEnv, 'persistent' | 'environmentType'>,
): string | null => {
  const problems: string[] = [];
  if (
    env.environmentType !== EnvironmentType.Container &&
    env.environmentType !== EnvironmentType.Standalone
  ) {
    problems.push(NOT_A_CONTAINER[env.environmentType] ?? 'not a container');
  }
  if (!env.persistent) problems.push('not persistent');
  return problems.length > 0 ? problems.join(' and ') : null;
};

export const jobEnvironmentError = (
  name: string | undefined,
  environments: readonly (TemplateFormEnv | undefined)[] | undefined,
): string | null => {
  if (!name) return 'Choose the environment that jobs run in';
  if (!JOB_ENVIRONMENT_NAME_PATTERN.test(name)) {
    return 'The environment name must be 4-12 characters: lowercase letters, digits, hyphens (no hyphens at start/end)';
  }
  const env = environments?.find(e => e?.name === name);
  if (!env) return `The environment ${name} is not in this Template`;
  const problem = jobEnvironmentProblem(env);
  return problem
    ? `${name} cannot run jobs: it is ${problem}. Jobs need a persistent Container or Standalone environment.`
    : null;
};

// The CRD's rules: Fixed needs a command; ImageDefault has none, which
// templateJobFromForm ensures.
export const jobCommandError = (
  policy: CommandPolicy | undefined,
  text: string | undefined,
  saved?: TemplateJobSpec | null,
): string | null =>
  policy === CommandPolicy.Fixed && !text?.trim() && !keptCommand(saved)
    ? 'Write the command that every job runs'
    : null;

export type WorkspaceJobLimit =
  | { state: 'loading' }
  | { state: 'error' }
  | {
      state: 'ready';
      queueLimits: { maxRunTime?: string | null } | null | undefined;
    };

// The hint under the time limit: the Workspace's limit, and the operator's rule
// (forge.CrownJobMaxRunTime): the smaller of the two applies.
export const jobRunTimeHint = (
  value: string | undefined,
  workspace: WorkspaceJobLimit,
): { text: string; warning: boolean } => {
  if (workspace.state === 'loading') {
    return { text: "Reading the Workspace's limit…", warning: false };
  }
  if (workspace.state === 'error') {
    return { text: "The Workspace's limit could not be read.", warning: true };
  }
  if (!workspace.queueLimits) {
    return {
      text: 'This Workspace has no job queue: its jobs cannot start.',
      warning: true,
    };
  }
  const limit = workspace.queueLimits.maxRunTime;
  const typed = value?.trim();
  if (!limit) {
    return {
      text: typed
        ? 'The Workspace sets no limit. Set-up included.'
        : "Empty: the operator's default applies. Set-up included.",
      warning: false,
    };
  }
  if (!typed) {
    return {
      text: `Empty: the Workspace's limit applies, ${limit}. Set-up included.`,
      warning: false,
    };
  }
  if (
    MAX_RUN_TIME_PATTERN.test(typed) &&
    effectiveRunLimit(typed, limit)?.source === 'Workspace'
  ) {
    return {
      text: `Above the Workspace's limit: jobs get ${limit}.`,
      warning: true,
    };
  }
  return {
    text: `The Workspace allows at most ${limit}. Set-up included.`,
    warning: false,
  };
};

// The words of the API for a save that failed. With onError on the hook,
// Apollo resolves the mutation with the ApolloError in errors.
export const saveErrorTexts = (errors: unknown): string[] => {
  if (errors instanceof ApolloError) return apiErrorTexts(errors);
  if (Array.isArray(errors) && errors.length > 0) {
    return apiErrorTexts(new ApolloError({ graphQLErrors: errors }));
  }
  if (errors instanceof Error) return [errors.message];
  return [];
};
