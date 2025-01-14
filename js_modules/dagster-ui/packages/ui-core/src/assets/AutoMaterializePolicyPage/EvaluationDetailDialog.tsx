import {
  Box,
  Button,
  Dialog,
  DialogFooter,
  Icon,
  Mono,
  NonIdealState,
  SpinnerWithText,
  Tag,
} from '@dagster-io/ui-components';
import {ReactNode, useMemo, useState} from 'react';

import {GET_SLIM_EVALUATIONS_QUERY} from './GetEvaluationsQuery';
import {PartitionTagSelector} from './PartitionTagSelector';
import {QueryfulEvaluationDetailTable} from './QueryfulEvaluationDetailTable';
import {
  GetSlimEvaluationsQuery,
  GetSlimEvaluationsQueryVariables,
} from './types/GetEvaluationsQuery.types';
import {usePartitionsForAssetKey} from './usePartitionsForAssetKey';
import {useQuery} from '../../apollo-client';
import {DEFAULT_TIME_FORMAT} from '../../app/time/TimestampFormat';
import {TimestampDisplay} from '../../schedules/TimestampDisplay';
import {AnchorButton} from '../../ui/AnchorButton';

interface Props {
  isOpen: boolean;
  onClose: () => void;
  assetKeyPath: string[];
  assetCheckName?: string;
  evaluationID: string;
}

export const EvaluationDetailDialog = ({
  isOpen,
  onClose,
  evaluationID,
  assetKeyPath,
  assetCheckName,
}: Props) => {
  return (
    <Dialog isOpen={isOpen} onClose={onClose} style={EvaluationDetailDialogStyle}>
      <EvaluationDetailDialogContents
        evaluationID={evaluationID}
        assetKeyPath={assetKeyPath}
        assetCheckName={assetCheckName}
        onClose={onClose}
      />
    </Dialog>
  );
};

interface ContentProps {
  evaluationID: string;
  assetKeyPath: string[];
  assetCheckName?: string;
  onClose: () => void;
}

const EvaluationDetailDialogContents = ({
  evaluationID,
  assetKeyPath,
  assetCheckName,
  onClose,
}: ContentProps) => {
  const [selectedPartition, setSelectedPartition] = useState<string | null>(null);

  const {data, loading} = useQuery<GetSlimEvaluationsQuery, GetSlimEvaluationsQueryVariables>(
    GET_SLIM_EVALUATIONS_QUERY,
    {
      variables: {
        assetKey: assetCheckName ? null : {path: assetKeyPath},
        assetCheckKey: assetCheckName
          ? {assetKey: {path: assetKeyPath}, name: assetCheckName}
          : null,
        cursor: `${BigInt(evaluationID) + 1n}`,
        limit: 2,
      },
    },
  );

  const {partitions: allPartitions, loading: partitionsLoading} =
    usePartitionsForAssetKey(assetKeyPath);

  const viewAllPath = useMemo(() => {
    // todo dish: I don't think the asset check evaluations list is permalinkable yet.
    if (assetCheckName) {
      return null;
    }

    const queryString = new URLSearchParams({
      view: 'automation',
      evaluation: evaluationID,
    }).toString();

    return `/assets/${assetKeyPath.join('/')}?${queryString}`;
  }, [assetCheckName, evaluationID, assetKeyPath]);

  if (loading || partitionsLoading) {
    return (
      <DialogContents
        header={<DialogHeader assetKeyPath={assetKeyPath} assetCheckName={assetCheckName} />}
        body={
          <Box padding={{top: 64}} flex={{direction: 'row', justifyContent: 'center'}}>
            <SpinnerWithText label="Loading evaluation details..." />
          </Box>
        }
        onDone={onClose}
      />
    );
  }

  const record = data?.assetConditionEvaluationRecordsOrError;

  if (record?.__typename === 'AutoMaterializeAssetEvaluationNeedsMigrationError') {
    return (
      <DialogContents
        header={<DialogHeader assetKeyPath={assetKeyPath} assetCheckName={assetCheckName} />}
        body={
          <Box margin={{top: 64}}>
            <NonIdealState
              icon="automation"
              title="Evaluation needs migration"
              description={record.message}
            />
          </Box>
        }
        onDone={onClose}
      />
    );
  }

  const evaluation = record?.records.find((r) => r.evaluationId === evaluationID);

  if (!evaluation) {
    return (
      <DialogContents
        header={<DialogHeader assetKeyPath={assetKeyPath} assetCheckName={assetCheckName} />}
        body={
          <Box margin={{top: 64}}>
            <NonIdealState
              icon="automation"
              title="Evaluation not found"
              description={
                <>
                  Evaluation <Mono>{evaluationID}</Mono> not found
                </>
              }
            />
          </Box>
        }
        onDone={onClose}
      />
    );
  }

  return (
    <DialogContents
      header={
        <>
          <DialogHeader
            assetKeyPath={assetKeyPath}
            assetCheckName={assetCheckName}
            timestamp={evaluation.timestamp}
          />
          {allPartitions.length > 0 && evaluation.isLegacy ? (
            <Box padding={{vertical: 12, right: 20}} flex={{justifyContent: 'flex-end'}}>
              <PartitionTagSelector
                allPartitions={allPartitions}
                selectedPartition={selectedPartition}
                selectPartition={setSelectedPartition}
              />
            </Box>
          ) : null}
        </>
      }
      body={
        <QueryfulEvaluationDetailTable
          evaluation={evaluation}
          assetKeyPath={assetKeyPath}
          selectedPartition={selectedPartition}
          setSelectedPartition={setSelectedPartition}
        />
      }
      viewAllButton={
        viewAllPath ? (
          <AnchorButton to={viewAllPath} icon={<Icon name="automation_condition" />}>
            View evaluations for this asset
          </AnchorButton>
        ) : null
      }
      onDone={onClose}
    />
  );
};

export const DialogHeader = ({
  assetKeyPath,
  assetCheckName,
  timestamp,
}: {
  assetKeyPath: string[];
  assetCheckName?: string;
  timestamp?: number;
}) => {
  const assetKeyPathString = assetKeyPath.join('/');
  const assetDetailsTag = assetCheckName ? (
    <Tag icon="asset_check">
      {assetCheckName} on {assetKeyPathString}
    </Tag>
  ) : (
    <Tag icon="asset">{assetKeyPathString}</Tag>
  );

  const timestampDisplay = timestamp ? (
    <TimestampDisplay
      timestamp={timestamp}
      timeFormat={{...DEFAULT_TIME_FORMAT, showSeconds: true}}
    />
  ) : null;

  return (
    <Box
      padding={{vertical: 16, horizontal: 20}}
      flex={{direction: 'row', alignItems: 'center', justifyContent: 'space-between'}}
      border="bottom"
    >
      <Box flex={{direction: 'row', alignItems: 'center', gap: 8}}>
        <Icon name="automation" />
        <strong>
          <span>Evaluation details</span>
          {timestampDisplay ? <span>: {timestampDisplay}</span> : ''}
        </strong>
      </Box>
      {assetDetailsTag}
    </Box>
  );
};

interface BasicContentProps {
  header: ReactNode;
  body: ReactNode;
  viewAllButton?: ReactNode;
  onDone: () => void;
}

// Dialog contents for which the body container is scrollable and expands to fill the height.
export const DialogContents = ({header, body, onDone, viewAllButton}: BasicContentProps) => {
  return (
    <Box flex={{direction: 'column'}} style={{height: '100%'}}>
      {header}
      <div style={{flex: 1, overflowY: 'auto'}}>{body}</div>
      <div style={{flexGrow: 0}}>
        <DialogFooter topBorder left={viewAllButton}>
          <Button onClick={onDone}>Done</Button>
        </DialogFooter>
      </div>
    </Box>
  );
};

const EvaluationDetailDialogStyle = {
  width: '80vw',
  maxWidth: '1400px',
  minWidth: '800px',
  height: '80vh',
  minHeight: '400px',
  maxHeight: '1400px',
};
