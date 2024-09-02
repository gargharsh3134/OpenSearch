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
import org.opensearch.common.Nullable;
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

    private final ShardStrategyPageToken nextToken;
    private final List<String> indicesFromRequestedToken;
    private final List<ShardRouting> shardRoutingsFromRequestedToken;

    public ShardBasedPaginationStrategy(
        @Nullable ShardStrategyPageToken requestedToken,
        int maxPageSize,
        String sortOrder,
        ClusterState clusterState
    ) {
        // Get sorted list of indices from metadata and filter out the required number of indices
        List<String> sortedIndicesList = PaginationStrategy.getListOfIndicesSortedByCreateTime(
            clusterState,
            sortOrder,
            requestedToken == null ? Long.MAX_VALUE : requestedToken.queryStartTime
        );
        Tuple<Tuple<Integer, List<String>>, List<ShardRouting>> detailsFromRequestedToken = getDetailsFromRequestedToken(
            requestedToken,
            sortedIndicesList,
            clusterState,
            maxPageSize,
            sortOrder
        );
        int newPageEndIndexNumber = detailsFromRequestedToken.v1().v1();
        this.indicesFromRequestedToken = detailsFromRequestedToken.v1().v2();
        this.shardRoutingsFromRequestedToken = detailsFromRequestedToken.v2();
        long queryStartTime = requestedToken == null
            ? DESCENDING_SORT_PARAM_VALUE.equals(sortOrder)
                ? clusterState.metadata().indices().get(sortedIndicesList.get(0)).getCreationDate()
                : clusterState.metadata().indices().get(sortedIndicesList.get(sortedIndicesList.size() - 1)).getCreationDate()
            : requestedToken.queryStartTime;

        if (newPageEndIndexNumber >= sortedIndicesList.size() || newPageEndIndexNumber < 0) {
            this.nextToken = null;
        } else {
            this.nextToken = new ShardStrategyPageToken(
                shardRoutingsFromRequestedToken.get(shardRoutingsFromRequestedToken.size() - 1).id(),
                newPageEndIndexNumber,
                clusterState.metadata().indices().get(sortedIndicesList.get(newPageEndIndexNumber - 1)).getCreationDate(),
                queryStartTime,
                sortedIndicesList.get(newPageEndIndexNumber - 1)
            );
        }
    }

    /**
     *
     * Used to extract out lastProcessedIndexNumber, indicesToBeQueried, shardRoutingResponseList
     */
    private Tuple<Tuple<Integer, List<String>>, List<ShardRouting>> getDetailsFromRequestedToken(
        final ShardStrategyPageToken requestedPageToken,
        final List<String> sortedIndicesList,
        final ClusterState clusterState,
        final int maxPageSize,
        final String sortOrder
    ) {
        final List<ShardRouting> shardRoutingResponseList = new ArrayList<>();
        final List<String> indicesToBeQueried = new ArrayList<>();

        // Since all the shards corresponding to the last processed index might not have been included in the last page,
        // start iterating from the last index number itself
        int newPageStartIndexNumber = requestedPageToken == null ? 0 : requestedPageToken.indexPosToStartPage;
        // Since all the shards for last ID would have already been sent in the last response,
        // start iterating from the next shard for current page
        int newPageStartShardID = requestedPageToken == null ? 0 : requestedPageToken.shardNumToStartPage + 1;
        if (newPageStartIndexNumber >= sortedIndicesList.size()
            || (newPageStartIndexNumber > 0
                && !Objects.equals(sortedIndicesList.get(newPageStartIndexNumber), requestedPageToken.nameOfLastRespondedIndex))) {
            // case denoting an already responded index has been deleted while the paginated queries are being executed
            // find the index whose creation time is just after/before the index which was last responded
            newPageStartIndexNumber = Math.min(newPageStartIndexNumber, sortedIndicesList.size() - 1);
            while (newPageStartIndexNumber > -1) {
                if (DESCENDING_SORT_PARAM_VALUE.equals(sortOrder)) {
                    if (newPageStartIndexNumber > 0
                        && clusterState.metadata()
                            .indices()
                            .get(sortedIndicesList.get(newPageStartIndexNumber))
                            .getCreationDate() > requestedPageToken.creationTimeOfLastRespondedIndex) {
                        break;
                    }
                } else {
                    if (newPageStartIndexNumber > 0
                        && clusterState.metadata()
                            .indices()
                            .get(sortedIndicesList.get(newPageStartIndexNumber))
                            .getCreationDate() < requestedPageToken.creationTimeOfLastRespondedIndex) {
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

    @Override
    @Nullable
    public PageToken getNextToken() {
        return nextToken;
    }

    @Override
    @Nullable
    public List<ShardRouting> getElementsFromRequestedToken() {
        return shardRoutingsFromRequestedToken;
    }

    public List<String> getIndicesFromRequestedToken() {
        return indicesFromRequestedToken;
    }

    /**
     * Token to be used by {@link IndexBasedPaginationStrategy}.
     * Token would like: IndexNumberToStartTheNextPageFrom + $ + CreationTimeOfLastRespondedIndex + $ +
     * QueryStartTime + $ + NameOfLastRespondedIndex
     */
    public static class ShardStrategyPageToken implements PageToken {

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
        public ShardStrategyPageToken(String requestedTokenString) {
            String decryptedToken = PageToken.decryptStringToken(requestedTokenString);
            final String[] decryptedTokenElements = decryptedToken.split("\\$");
            if (decryptedTokenElements.length != 5) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }
            try {
                this.shardNumToStartPage = Integer.parseInt(decryptedTokenElements[0]);
                this.indexPosToStartPage = Integer.parseInt(decryptedTokenElements[1]);
                this.creationTimeOfLastRespondedIndex = Long.parseLong(decryptedTokenElements[2]);
                this.queryStartTime = Long.parseLong(decryptedTokenElements[3]);
                this.nameOfLastRespondedIndex = decryptedTokenElements[4];
                if (shardNumToStartPage < 0 || indexPosToStartPage < 0 || creationTimeOfLastRespondedIndex < 0 || queryStartTime < 0) {
                    throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
                }
            } catch (NumberFormatException exception) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }
        }

        public ShardStrategyPageToken(
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

        @Override
        public String generateEncryptedToken() {
            return PageToken.encryptStringToken(
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
    }

}
