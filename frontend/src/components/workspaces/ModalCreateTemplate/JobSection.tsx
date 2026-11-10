import { InfoCircleOutlined } from '@ant-design/icons';
import {
  Form,
  Input,
  InputNumber,
  Radio,
  Select,
  Space,
  Switch,
  Tooltip,
  Typography,
} from 'antd';
import { type FC, useEffect, useMemo } from 'react';
import {
  CommandPolicy,
  useWorkspaceJobLimitQuery,
} from '../../../generated-types';
import {
  COMMAND_MAX_LENGTH,
  MAX_RUN_TIME_PATTERN,
} from '../../activePage/TableInstance/RunJob/crownJobUtils';
import {
  JOB_RESOURCE_FIELDS,
  JOB_RESOURCE_MAX,
  jobCommandError,
  jobEnvironmentError,
  jobEnvironmentProblem,
  jobRunTimeHint,
  keptCommand,
  splitJobResources,
  type WorkspaceJobLimit,
} from './templateJob';
import type { TemplateFormEnv, TemplateJobSpec } from './types';
import { formItemLayout } from './utils';

const POLICIES = [
  {
    value: CommandPolicy.Editable,
    title: 'The student types the command',
    line: 'Each job runs the command the student writes; the command below, if any, pre-fills it.',
  },
  {
    value: CommandPolicy.Fixed,
    title: 'Always this command',
    line: 'Every job runs the command below; the student cannot change it.',
  },
  {
    value: CommandPolicy.ImageDefault,
    title: 'The image decides',
    line: "Jobs run the image's own entrypoint, with CROWNLABS_JOB=true; no command is passed.",
  },
];

const resourceInputField = {
  labelCol: { span: 24 },
  wrapperCol: { span: 24 },
};

export interface IJobSectionProps {
  // The Workspace whose job queue limits the time.
  workspaceName?: string;
  // On edit, spec.job as it was read.
  savedJob?: TemplateJobSpec | null;
  // Whether the editor is open: the Workspace is read only then.
  show: boolean;
}

export const JobSection: FC<IJobSectionProps> = ({
  workspaceName,
  savedJob,
  show,
}) => {
  const form = Form.useFormInstance();
  const enabled: boolean = Form.useWatch(['job', 'enabled'], form) ?? false;
  const policy: CommandPolicy =
    Form.useWatch(['job', 'commandPolicy'], form) ?? CommandPolicy.Editable;
  const maxRunTime: string | undefined = Form.useWatch(
    ['job', 'maxRunTime'],
    form,
  );
  const environments: (TemplateFormEnv | undefined)[] | undefined =
    Form.useWatch('environments', form);

  const { data, loading, error } = useWorkspaceJobLimitQuery({
    variables: { name: workspaceName ?? '' },
    skip: !show || !enabled || !workspaceName,
  });
  const workspaceLimit: WorkspaceJobLimit = error
    ? { state: 'error' }
    : loading || !data
      ? { state: 'loading' }
      : { state: 'ready', queueLimits: data.workspace?.spec?.queueLimits };
  const hint = jobRunTimeHint(maxRunTime, workspaceLimit);

  const options = useMemo(
    () =>
      (environments ?? [])
        .filter((env): env is TemplateFormEnv => Boolean(env?.name))
        .map(env => {
          const problem = jobEnvironmentProblem(env);
          return {
            value: env.name,
            disabled: problem !== null,
            label: problem ? `${env.name} (${problem})` : env.name,
          };
        }),
    [environments],
  );
  const qualifying = options.filter(option => !option.disabled);
  const onlyQualifying =
    qualifying.length === 1 ? qualifying[0].value : undefined;

  // The only environment that qualifies is pre-selected. setFieldValue, unlike
  // setFieldsValue, does not mark the field as touched.
  useEffect(() => {
    if (
      enabled &&
      onlyQualifying &&
      !form.getFieldValue(['job', 'environmentName'])
    ) {
      form.setFieldValue(['job', 'environmentName'], onlyQualifying);
    }
  }, [enabled, onlyQualifying, form]);

  // The chosen environment is checked again when the environments change.
  useEffect(() => {
    if (enabled && form.getFieldValue(['job', 'environmentName'])) {
      form.validateFields([['job', 'environmentName']]).catch(() => undefined);
    }
  }, [enabled, environments, form]);

  const kept = keptCommand(savedJob);
  const keptResources = Object.entries(
    splitJobResources(savedJob?.otherResources).kept,
  );

  return (
    <>
      <Form.Item
        name={['job', 'enabled']}
        valuePropName="checked"
        label="Allow jobs from this Template"
        {...formItemLayout}
      >
        <Switch />
      </Form.Item>
      {enabled && (
        <>
          <Form.Item
            name={['job', 'environmentName']}
            label="Environment"
            extra="A job runs a copy of the instance of this environment, with its disk."
            rules={[
              {
                validator: async (_, value: string | undefined) => {
                  const problem = jobEnvironmentError(
                    value,
                    form.getFieldValue('environments'),
                  );
                  if (problem) throw new Error(problem);
                },
              },
            ]}
            {...formItemLayout}
          >
            <Select
              options={options}
              placeholder="Choose an environment"
              getPopupContainer={trigger =>
                trigger.parentElement || document.body
              }
            />
          </Form.Item>
          <Form.Item
            name={['job', 'commandPolicy']}
            label="Command policy"
            {...formItemLayout}
          >
            <Radio.Group>
              <Space direction="vertical">
                {POLICIES.map(option => (
                  <Radio key={option.value} value={option.value}>
                    <div>{option.title}</div>
                    <Typography.Text type="secondary">
                      {option.line}
                    </Typography.Text>
                  </Radio>
                ))}
              </Space>
            </Radio.Group>
          </Form.Item>
          {policy !== CommandPolicy.ImageDefault && (
            <>
              {kept && (
                <Form.Item
                  label="Command kept"
                  extra="Not of the form sh -c: it is kept unless you type a new command below."
                  {...formItemLayout}
                >
                  <Space wrap size={[4, 4]}>
                    {kept.map((arg, index) => (
                      <Typography.Text key={index} code>
                        {arg}
                      </Typography.Text>
                    ))}
                  </Space>
                </Form.Item>
              )}
              <Form.Item
                name={['job', 'commandText']}
                label={
                  policy === CommandPolicy.Fixed
                    ? 'Command'
                    : 'Command (optional)'
                }
                extra={
                  policy === CommandPolicy.Fixed
                    ? 'runs with sh -c'
                    : "runs with sh -c; it pre-fills the student's box"
                }
                dependencies={[['job', 'commandPolicy']]}
                rules={[
                  {
                    validator: async (_, value: string | undefined) => {
                      const problem = jobCommandError(
                        form.getFieldValue(['job', 'commandPolicy']),
                        value,
                        savedJob,
                      );
                      if (problem) throw new Error(problem);
                    },
                  },
                ]}
                {...formItemLayout}
              >
                <Input.TextArea
                  autoSize={{ minRows: 2, maxRows: 6 }}
                  maxLength={COMMAND_MAX_LENGTH}
                  placeholder="python train.py"
                />
              </Form.Item>
            </>
          )}
          <Form.Item
            name={['job', 'maxRunTime']}
            label="Time limit (optional)"
            extra={
              <Typography.Text type={hint.warning ? 'warning' : 'secondary'}>
                {hint.text}
              </Typography.Text>
            }
            rules={[
              {
                pattern: MAX_RUN_TIME_PATTERN,
                message: 'A number followed by m, h or d (e.g., 30m, 6h, 2d)',
              },
            ]}
            {...formItemLayout}
          >
            <Input placeholder="e.g., 30m" allowClear />
          </Form.Item>
          <Typography.Paragraph className="mb-0">
            Extra resources for each job{' '}
            <Tooltip title="Requested by each job in addition to the resources of its environment">
              <InfoCircleOutlined className="ml-1" />
            </Tooltip>
          </Typography.Paragraph>
          <Space direction="horizontal" className="ml-2 mb-4">
            {JOB_RESOURCE_FIELDS.map(field => (
              <Form.Item
                key={field.key}
                name={['job', 'otherResources', field.key]}
                label={field.label}
                {...resourceInputField}
              >
                <InputNumber
                  step={1}
                  style={{ width: '120px', textAlignLast: 'center' }}
                  min={0}
                  max={JOB_RESOURCE_MAX}
                  addonAfter="#"
                />
              </Form.Item>
            ))}
          </Space>
          {keptResources.length > 0 && (
            <Typography.Paragraph type="secondary">
              Also kept as they are:{' '}
              {keptResources.map(([name, value]) => (
                <Typography.Text key={name} code>
                  {`${name}: ${value}`}
                </Typography.Text>
              ))}
            </Typography.Paragraph>
          )}
        </>
      )}
    </>
  );
};
