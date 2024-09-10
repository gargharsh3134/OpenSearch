/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.pagination;

import org.opensearch.OpenSearchParseException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This strategy can be used by the Rest APIs wanting to paginate the responses based on Indices.
 * The strategy considers create timestamps of indices as the keys to iterate over pages.
 *
 * @opensearch.internal
 */
public class ShardBasedPaginationStrategy implements PaginationStrategy<ShardRouting> {

    private final PaginatedQueryResponse paginatedQueryResponse;
    private final List<String> indicesFromRequestedToken;
    private final List<ShardRouting> shardRoutingsFromRequestedToken;

    public ShardBasedPaginationStrategy(PaginatedQueryRequest paginatedQueryRequest, String paginatedElement, ClusterState clusterState) {
        ShardStrategyTokenParser requestedToken = paginatedQueryRequest.getRequestedTokenStr() == null
            ? null
            : new ShardStrategyTokenParser(paginatedQueryRequest.getRequestedTokenStr());

        // Get sorted list of indices not containing the ones created after query start time
        List<String> sortedIndicesList = PaginationStrategy.getListOfIndicesSortedByCreateTime(
            clusterState,
            paginatedQueryRequest.getSort(),
            requestedToken == null ? Long.MAX_VALUE : requestedToken.queryStartTime
        );
        if (sortedIndicesList.isEmpty()) {
            // Denotes, that all the indices which were created before the queryStartTime have been deleted.
            // No nextToken and indices need to be shown in such cases.
            this.indicesFromRequestedToken = new ArrayList<>();
            this.shardRoutingsFromRequestedToken = new ArrayList<>();
            this.paginatedQueryResponse = new PaginatedQueryResponse(null, paginatedElement);
        } else {
            Tuple<Tuple<Integer, List<String>>, List<ShardRouting>> detailsFromRequestedToken = getDetailsFromRequestedToken(
                requestedToken,
                sortedIndicesList,
                clusterState,
                paginatedQueryRequest.getSize(),
                paginatedQueryRequest.getSort()
            );
            int requestedPageEndIndexNumber = detailsFromRequestedToken.v1().v1();
            this.indicesFromRequestedToken = detailsFromRequestedToken.v1().v2();
            this.shardRoutingsFromRequestedToken = detailsFromRequestedToken.v2();

            // Set the queryStart time as the timestamp of latest created index if requested token is null.
            long queryStartTime = requestedToken == null
                ? DESCENDING_SORT_PARAM_VALUE.equals(paginatedQueryRequest.getSort())
                    ? clusterState.metadata().indices().get(sortedIndicesList.get(0)).getCreationDate()
                    : clusterState.metadata().indices().get(sortedIndicesList.get(sortedIndicesList.size() - 1)).getCreationDate()
                : requestedToken.queryStartTime;

            this.paginatedQueryResponse = new PaginatedQueryResponse(
                requestedPageEndIndexNumber >= sortedIndicesList.size()
                    ? null
                    : new ShardStrategyTokenParser(
                        shardRoutingsFromRequestedToken.get(shardRoutingsFromRequestedToken.size() - 1).id(),
                        requestedPageEndIndexNumber,
                        clusterState.metadata().indices().get(sortedIndicesList.get(requestedPageEndIndexNumber - 1)).getCreationDate(),
                        queryStartTime,
                        sortedIndicesList.get(requestedPageEndIndexNumber - 1)
                    ).generateEncryptedToken(),
                paginatedElement
            );
        }
    }

    @Override
    public PaginatedQueryResponse getPaginatedQueryResponse() {
        return paginatedQueryResponse;
    }

    @Override
    public List<ShardRouting> getElementsFromRequestedToken() {
        return shardRoutingsFromRequestedToken;
    }

    public List<String> getIndicesFromRequestedToken() {
        return indicesFromRequestedToken;
    }

    /**
     *
     * Used to extract out lastProcessedIndexNumber, indicesToBeQueried, shardRoutingResponseList
     */
    private Tuple<Tuple<Integer, List<String>>, List<ShardRouting>> getDetailsFromRequestedToken(
        final ShardStrategyTokenParser requestedTokenParser,
        final List<String> sortedIndicesList,
        final ClusterState clusterState,
        final int maxPageSize,
        final String sortOrder
    ) {
        final List<ShardRouting> shardRoutingResponseList = new ArrayList<>();
        final List<String> indicesToBeQueried = new ArrayList<>();

        // Since all the shards corresponding to the last processed index might not have been included in the last page,
        // start iterating from the last index number itself
        int newPageStartIndexNumber = requestedTokenParser == null ? 0 : requestedTokenParser.indexPosToStartPage;
        // Since all the shards for last ID would have already been sent in the last response,
        // start iterating from the next shard for current page
        int newPageStartShardID = requestedTokenParser == null ? 0 : requestedTokenParser.shardNumToStartPage + 1;
        if (newPageStartIndexNumber >= sortedIndicesList.size()
            || (newPageStartIndexNumber > 0
                && !Objects.equals(sortedIndicesList.get(newPageStartIndexNumber), requestedTokenParser.nameOfLastRespondedIndex))) {
            // case denoting an already responded index has been deleted while the paginated queries are being executed
            // find the index whose creation time is just after/before the index which was last responded
            newPageStartIndexNumber = Math.min(newPageStartIndexNumber, sortedIndicesList.size() - 1);
            while (newPageStartIndexNumber > -1) {
                if (DESCENDING_SORT_PARAM_VALUE.equals(sortOrder)) {
                    if (newPageStartIndexNumber > 0
                        && clusterState.metadata()
                            .indices()
                            .get(sortedIndicesList.get(newPageStartIndexNumber))
                            .getCreationDate() > requestedTokenParser.creationTimeOfLastRespondedIndex) {
                        break;
                    }
                } else {
                    if (newPageStartIndexNumber > 0
                        && clusterState.metadata()
                            .indices()
                            .get(sortedIndicesList.get(newPageStartIndexNumber))
                            .getCreationDate() < requestedTokenParser.creationTimeOfLastRespondedIndex) {
                        break;
                    }
                }
                newPageStartIndexNumber--;
            }

            if (newPageStartIndexNumber < 0) {
                return new Tuple<>(new Tuple<>(newPageStartIndexNumber, indicesToBeQueried), shardRoutingResponseList);
            }

            newPageStartShardID = 0;
        }

        // Get the number of shards upto the maxPageSize
        long shardCountSoFar = 0L;
        int indexNumberInSortedList = newPageStartIndexNumber;
        for (; indexNumberInSortedList < sortedIndicesList.size(); indexNumberInSortedList++) {
            String index = sortedIndicesList.get(indexNumberInSortedList);
            Map<Integer, IndexShardRoutingTable> indexShards = clusterState.getRoutingTable().getIndicesRouting().get(index).getShards();
            // If all the shards corresponding to the last index were already processed, move to the next Index
            if (indexNumberInSortedList == newPageStartIndexNumber && (newPageStartShardID > indexShards.size() - 1)) {
                newPageStartShardID = 0;
                continue;
            }
            int lastProcessedShardNumberForCurrentIndex = -1;
            int shardID = (indexNumberInSortedList == newPageStartIndexNumber) ? newPageStartShardID : 0;
            for (; shardID < indexShards.size(); shardID++) {
                shardCountSoFar += indexShards.get(shardID).shards().size();
                if (shardCountSoFar > maxPageSize) {
                    break;
                }
                shardRoutingResponseList.addAll(indexShards.get(shardID).shards());
                lastProcessedShardNumberForCurrentIndex = shardID;
            }

            if (shardCountSoFar > maxPageSize) {
                if (lastProcessedShardNumberForCurrentIndex != -1) {
                    indicesToBeQueried.add(index);
                }
                break;
            }
            indicesToBeQueried.add(index);
        }
        return new Tuple<>(new Tuple<>(indexNumberInSortedList, indicesToBeQueried), shardRoutingResponseList);
    }

    /**
     * Token to be used by {@link ShardBasedPaginationStrategy}.
     * Token would like: ShardIdOfLastRespondedShard + $ + IndexNumberToStartTheNextPageFrom + $ +
     * CreationTimeOfLastRespondedIndex + $ + QueryStartTime + $ + NameOfLastRespondedIndex
     */
    public static class ShardStrategyTokenParser {

        private final int shardNumToStartPage;
        private final int indexPosToStartPage;
        private final long creationTimeOfLastRespondedIndex;
        private final long queryStartTime;
        private final String nameOfLastRespondedIndex;

        /**
         * Will perform simple validations on token received in the request and initialize the data members.
         * The token should be base64 encoded, and should contain the expected number of elements separated by "$".
         * The timestamps should also be a valid long.
         *
         * @param requestedTokenString string denoting the encoded next token requested by the user
         */
        public ShardStrategyTokenParser(String requestedTokenString) {
            validateShardStrategyToken(requestedTokenString);
            String decryptedToken = PaginationStrategy.decryptStringToken(requestedTokenString);
            final String[] decryptedTokenElements = decryptedToken.split("\\$");
            this.shardNumToStartPage = Integer.parseInt(decryptedTokenElements[0]);
            this.indexPosToStartPage = Integer.parseInt(decryptedTokenElements[1]);
            this.creationTimeOfLastRespondedIndex = Long.parseLong(decryptedTokenElements[2]);
            this.queryStartTime = Long.parseLong(decryptedTokenElements[3]);
            this.nameOfLastRespondedIndex = decryptedTokenElements[4];
        }

        public ShardStrategyTokenParser(
            int shardNumToStartPage,
            int indexPositionToStartPageFrom,
            long creationTimeOfLastRespondedIndex,
            long queryStartTime,
            String nameOfLastRespondedIndex
        ) {
            Objects.requireNonNull(nameOfLastRespondedIndex, "index name should be provided");
            this.shardNumToStartPage = shardNumToStartPage;
            this.indexPosToStartPage = indexPositionToStartPageFrom;
            this.creationTimeOfLastRespondedIndex = creationTimeOfLastRespondedIndex;
            this.queryStartTime = queryStartTime;
            this.nameOfLastRespondedIndex = nameOfLastRespondedIndex;
        }

        public String generateEncryptedToken() {
            return PaginationStrategy.encryptStringToken(
                shardNumToStartPage
                    + "$"
                    + indexPosToStartPage
                    + "$"
                    + creationTimeOfLastRespondedIndex
                    + "$"
                    + queryStartTime
                    + "$"
                    + nameOfLastRespondedIndex
            );
        }

        /**
         * Will perform simple validations on token received in the request and initialize the data members.
         * The token should be base64 encoded, and should contain the expected number of elements separated by "$".
         * The timestamps should also be a valid long.
         *
         * @param requestedTokenString string denoting the encoded next token requested by the user
         */
        public static void validateShardStrategyToken(String requestedTokenString) {
            String decryptedToken = PaginationStrategy.decryptStringToken(requestedTokenString);
            final String[] decryptedTokenElements = decryptedToken.split("\\$");
            if (decryptedTokenElements.length != 5) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }
            try {
                int shardNumToStartPage = Integer.parseInt(decryptedTokenElements[0]);
                int indexPosToStartPage = Integer.parseInt(decryptedTokenElements[1]);
                long creationTimeOfLastRespondedIndex = Long.parseLong(decryptedTokenElements[2]);
                long queryStartTime = Long.parseLong(decryptedTokenElements[3]);
                if (shardNumToStartPage < 0 || indexPosToStartPage < 0 || creationTimeOfLastRespondedIndex < 0 || queryStartTime < 0) {
                    throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
                }
            } catch (NumberFormatException exception) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }
        }
    }

}
