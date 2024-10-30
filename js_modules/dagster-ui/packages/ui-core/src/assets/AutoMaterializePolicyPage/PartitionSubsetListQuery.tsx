import {gql} from '../../apollo-client';

export const PARTITION_SUBSET_LIST_QUERY = gql`
  query PartitionSubsetListQuery(
    $assetKey: AssetKeyInput!
    $evaluationId: BigInt!
    $nodeUniqueId: String!
  ) {
    truePartitionsForAutomationConditionEvaluationNode(
      assetKey: $assetKey
      evaluationId: $evaluationId
      nodeUniqueId: $nodeUniqueId
    )
  }
`;
