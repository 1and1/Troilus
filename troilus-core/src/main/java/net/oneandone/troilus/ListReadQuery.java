/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.troilus;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;


import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.reactivestreams.Publisher;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;




 
/**
 * The list read query implementation
 */
class ListReadQuery extends AbstractQuery<ListReadQuery> implements ListReadWithUnit<ResultList<Record>, Record> {
    
    private final ReadQueryData data;
  
    
    /**
     * @param ctx   the context 
     * @param data  the data
     */
    ListReadQuery(Context ctx, ReadQueryData data) {
        super(ctx);
        this.data = data;
    }

    
    ////////////////////
    // factory methods

    @Override
    protected ListReadQuery newQuery(Context newContext) {
        return new ListReadQuery(newContext, data);
    }
    
    private ListReadQuery newQuery(ReadQueryData data) {
        return new ListReadQuery(getContext(), data);
    }

    //
    ////////////////////

    
    
    @Override
    public ListReadQuery all() {
        return newQuery(data.columnsToFetch(ImmutableMap.<String, Boolean>of()));
    }
    
    private ListReadQuery columns(ImmutableCollection<String> namesToRead) {
        ListReadQuery read = this;
        for (String columnName : namesToRead) {
            read = read.column(columnName);
        }
        return read;
    }
    
    @Override
    public ListReadQuery column(String name) {
        return newQuery(data.columnsToFetch(Immutables.join(data.getColumnsToFetch(), name, false)));
    }
    
    @Override
    public ListReadQuery columnWithMetadata(String name) {
        return newQuery(data.columnsToFetch(Immutables.join(data.getColumnsToFetch(), name, true)));
    }
    
    @Override
    public ListReadQuery columns(String... names) {
        return columns(ImmutableSet.copyOf(names));
    }
    
    @Override
    public ListReadQuery column(ColumnName<?> name) {
        return column(name.getName());
    }
    
    @Override
    public ListReadQuery columnWithMetadata(ColumnName<?> name) {
        return columnWithMetadata(name.getName());
    }
    
    @Override
    public ListReadQuery columns(ColumnName<?>... names) {
        final List<String> ns = Lists.newArrayList();
        for (ColumnName<?> name : names) {
            ns.add(name.getName());
        }
        return columns(ImmutableList.copyOf(ns));
    }

    @Override
    public ListReadQuery withLimit(int limit) {
        return newQuery(data.limit(limit));
    }
    
    @Override
    public ListReadQuery withAllowFiltering() {
        return newQuery(data.allowFiltering(true));
    }

    @Override
    public ListReadQuery withFetchSize(int fetchSize) {
        return newQuery(data.fetchSize(fetchSize));
    }
    
    @Override
    public ListReadQuery withDistinct() {
        return newQuery(data.distinct(true));
    }
    
    @Override
    public ListReadQuery withPagingState(PagingState pagingState) {
        return newQuery(data.pagingState(pagingState));
    }
    
    @Override
    public CountReadQuery count() {
        return new CountReadQuery(getContext(), new CountReadQueryData(data.getTablename())
                                                                        .whereConditions(data.getWhereConditions())
                                                                        .limit(data.getLimit())
                                                                        .fetchSize(data.getFetchSize())
                                                                        .allowFiltering(data.getAllowFiltering())
                                                                        .distinct(data.getDistinct()));
    }
    
    @Override
    public <E> ListEntityReadQuery<E> asEntity(Class<E> objectClass) {
        return new ListEntityReadQuery<>(getContext(), this, objectClass) ;
    }
    
    
    @Override
    public Publisher<Record> executeRx() {
        CompletableFuture<ResultList<Record>> recordsFuture = executeAsync();
        return new ResultListPublisher<>(recordsFuture);
    }
    
    @Override
    public ResultList<Record> execute() {
        return ListenableFutures.getUninterruptibly(executeAsync());
    }
    
    @Override
    public CompletableFuture<ResultList<Record>> executeAsync() {
        return executeAsync(data, getDefaultDbSession());
    }

    
    private CompletableFuture<ResultList<Record>> executeAsync(final ReadQueryData queryData, DBSession dbSession) {
        final ListenableFuture<ResultSet> resultSetFuture = performAsync(dbSession, toStatementAsync(queryData, getUDTValueMapper(), dbSession));
    	
        // result set to record list mapper
        final Function<ResultSet, ResultList<Record>> resultSetToRecordList = new Function<ResultSet, ResultList<Record>>() {
            
            @Override
            public ResultList<Record> apply(ResultSet resultSet) {
                return new RecordListImpl(getContext(), queryData, resultSet);
            }
        };
        final ListenableFuture<ResultList<Record>> recordListFuture =  Futures.transform(resultSetFuture, resultSetToRecordList); 
        
        // running interceptors within dedicated threads!
        return executeResponseInterceptorsAsync(queryData, recordListFuture);
    }


    /**
     * Prepares the Statement for Pagination, if fetchSize is set. Otherwise, it simply  
     * returns ReadQueryDataImpl.toStatementAsync(data, udtValueMapper, dbSession)
     * as in the original code did in executeAsync().
     * 
     * @param queryData
     * @param udtValueMapper
     * @param dbSession
     * 
     * @return ListenableFuture<Statement>
     */
    private CompletableFuture<Statement> toStatementAsync(final ReadQueryData queryData, UDTValueMapper udtValueMapper, DBSession dbSession) {
        final CompletableFuture<Statement> lfs = ReadQueryDataImpl.toStatementAsync(data, udtValueMapper, dbSession);
    	
    	final  Integer fetchSize = data.getFetchSize();
    	if (fetchSize != null) {
    		Statement statement = null;
    		try {
    			statement = lfs.get();
    		} catch (InterruptedException | ExecutionException e) {
    			throw new RuntimeException("Failed to get the Statement from ListenableFuture<Statement>", e);
    		}
    		
			// The FetchSize is lost somehow when ReadQueryData.toStatementAsync() is invoked.
    		// In the debugger, it was always zero.  So, it is reset here directly on the Statement
			// This sets the fetchsize specifically on the Statement before executing.
			statement.setFetchSize(fetchSize);
			
			// The PagingState is not set during ReadQueryData.toStatementAsync() because
			// the driver compares the Select (a RegularStatement) to the previous PagingState's
			// BoundStatement and fails the hash() check with a PagingStateException.
			// So, like the fetch size, the PagingState must be done here.
			statement.setPagingState(data.getPagingState());
		}
    	return lfs;
    }
    
    /**
     * The entity list read implementation
     * @param <E> the entity type
     */
    static class ListEntityReadQuery<E> extends AbstractQuery<ListEntityReadQuery<E>> implements ListRead<ResultList<E>, E> {
        private final ListReadQuery query;
        private final Class<E> clazz;
        
        /**
         * @param ctx   the context
         * @param query the query 
         * @param clazz the entity type
         */
        ListEntityReadQuery(Context ctx, ListReadQuery query, Class<E> clazz) {
            super(ctx);
            this.query = query;
            this.clazz = clazz;
        }
        
        @Override
        protected ListEntityReadQuery<E> newQuery(Context newContext) {
            return new ListReadQuery(newContext, query.data).asEntity(clazz);
        }

        @Override
        public ListEntityReadQuery<E> withDistinct() {
            return query.withDistinct().asEntity(clazz);
        }
        
        @Override
        public ListEntityReadQuery<E> withFetchSize(int fetchSize) {
            return query.withFetchSize(fetchSize).asEntity(clazz);
        }
        
        @Override
        public ListEntityReadQuery<E> withAllowFiltering() {
            return query.withAllowFiltering().asEntity(clazz);
        }
        
        @Override
        public ListEntityReadQuery<E> withLimit(int limit) {
            return query.withLimit(limit).asEntity(clazz);
        }
        
        @Override
        public ResultList<E> execute() {
            return ListenableFutures.getUninterruptibly(executeAsync());
        }

        @Override
        public CompletableFuture<ResultList<E>> executeAsync() {
            return query.executeAsync();
            
            final Function<ResultList<Record>, ResultList<E>> mapEntity = new Function<ResultList<Record>, ResultList<E>>() {
                @Override
                public ResultList<E> apply(ResultList<Record> recordList) {
                    return new EntityListImpl<>(query.data.getTablename(), getBeanMapper(), getCatalog(), recordList, clazz);
                }
            };
            
            return Futures.transform(future, mapEntity);
        }
        
        @Override
        public Publisher<E> executeRx() {
            final ListenableFuture<ResultList<E>> recordsFuture = executeAsync();
            return new ResultListPublisher<>(recordsFuture);
        }

		@Override
		public ListEntityReadQuery<E> withPagingState(PagingState pagingState) {
			return query.withPagingState(pagingState).asEntity(clazz);
		}


    }
    
    
    private static class EntityListImpl<F> extends ResultAdapter implements ResultList<F> {
        private final Tablename tablename;
        private final BeanMapper beanMapper;
        private final MetadataCatalog catalog;
        private final ResultList<Record> recordList;
        private final Class<F> clazz;
    
        EntityListImpl(Tablename tablename, BeanMapper beanMapper, MetadataCatalog catalog, ResultList<Record> recordList, Class<F> clazz) {
            super(recordList);
            this.tablename = tablename;
            this.beanMapper = beanMapper;
            this.catalog = catalog;
            this.recordList = recordList;
            this.clazz = clazz;
        }
        
        public FetchingIterator<F> iterator() {
            
            return new FetchingIterator<F>() {
                private final FetchingIterator<Record> recordIt = recordList.iterator();
                
                @Override
                public boolean hasNext() {
                    return recordIt.hasNext();
                }
                
                @Override
                public F next() {
                    return beanMapper.fromValues(clazz, RecordImpl.toPropertiesSource(recordIt.next()), catalog.getColumnNames(tablename));
                }
                
                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
                
                @Override
                public int getAvailableWithoutFetching() {
                    return recordIt.getAvailableWithoutFetching();
                }
                
                @Override
                public boolean isFullyFetched() {
                    return recordIt.isFullyFetched();
                }
                
                @Override
                public CompletableFuture<ResultSet> fetchMoreResultsAsync() {
                    return recordIt.fetchMoreResultsAsync();
                }
            };
        }
    }
    
    
    private static final class CountReadQueryData {
        final Tablename tablename;
        final ImmutableSet<Clause> whereClauses;
        final Integer limit;
        final Boolean allowFiltering;
        final Integer fetchSize;
        final Boolean distinct;

        
        
        public CountReadQueryData(Tablename tablename) {
            this(tablename,
                 ImmutableSet.<Clause>of(),
                 null,
                 null,
                 null,
                 null);
        }
        
        private CountReadQueryData(Tablename tablename,
                                   ImmutableSet<Clause> whereClauses, 
                                   Integer limit, 
                                   Boolean allowFiltering,
                                   Integer fetchSize,
                                   Boolean distinct) {
            this.tablename = tablename;
            this.whereClauses = whereClauses;
            this.limit = limit;
            this.allowFiltering = allowFiltering;
            this.fetchSize = fetchSize;
            this.distinct = distinct;
        }
        

        
        public CountReadQueryData whereConditions(ImmutableSet<Clause> whereClauses) {
            return new CountReadQueryData(this.tablename,
                                          whereClauses,
                                          this.limit,
                                          this.allowFiltering,
                                          this.fetchSize,
                                          this.distinct);  
        }


        
        public CountReadQueryData limit(Integer limit) {
            return new CountReadQueryData(this.tablename,
                                          this.whereClauses,
                                          limit,
                                          this.allowFiltering,
                                          this.fetchSize,
                                          this.distinct);  
        }

        
        public CountReadQueryData allowFiltering(Boolean allowFiltering) {
            return new CountReadQueryData(this.tablename,
                                          this.whereClauses,
                                          this.limit,
                                          allowFiltering,
                                          this.fetchSize,
                                          this.distinct);  
        }

        
        public CountReadQueryData fetchSize(Integer fetchSize) {
            return new CountReadQueryData(this.tablename,
                                          this.whereClauses,
                                          this.limit,
                                          this.allowFiltering,
                                          fetchSize,
                                          this.distinct);  
        }

        
        public CountReadQueryData distinct(Boolean distinct) {
            return new CountReadQueryData(this.tablename,
                                          this.whereClauses,
                                          this.limit,
                                          this.allowFiltering,
                                          this.fetchSize,
                                          distinct);  
        }
        
        public Tablename getTablename() {
            return tablename;
        }
        
        public ImmutableSet<Clause> getWhereConditions() {
            return whereClauses;
        }

        public Integer getLimit() {
            return limit;
        }

        public Boolean getAllowFiltering() {
            return allowFiltering;
        }

        public Integer getFetchSize() {
            return fetchSize;
        }

        public Boolean getDistinct() {
            return distinct;
        }
    }


    
    static class CountReadQuery extends AbstractQuery<CountReadQuery> implements ListRead<Count, Count> {
        
        private final CountReadQueryData data;
    
    
        public CountReadQuery(Context ctx, CountReadQueryData data) {
            super(ctx);
            this.data = data;
        }
    
        @Override
        protected CountReadQuery newQuery(Context newContext) {
            return new CountReadQuery(newContext, data);
        }
        
        @Override
        public CountReadQuery withLimit(int limit) {
            return new CountReadQuery(getContext(),
                                      data.limit(limit)); 
        }
        
        
        @Override
        public CountReadQuery withAllowFiltering() {
            return new CountReadQuery(getContext(),
                                      data.allowFiltering(true)); 
        }
    
        @Override
        public CountReadQuery withFetchSize(int fetchSize) {
            return new CountReadQuery(getContext(),
                                      data.fetchSize(fetchSize));
        }
        
        @Override
        public CountReadQuery withDistinct() {
            return new CountReadQuery(getContext(),
                                      data.distinct(true));
        }
    
    
        
        private Statement toStatement(CountReadQueryData queryData) {
            Select.Selection selection = select();
            
            if (queryData.getDistinct() != null) {
                if (queryData.getDistinct()) {
                    selection.distinct(); 
                };
            }
    
     
            selection.countAll();
            
            Select select = selection.from(data.getTablename().getTablename());
            
            for (Clause whereCondition : queryData.getWhereConditions()) {
                select.where(whereCondition);
            }
            
            if (queryData.getLimit() != null) {
                select.limit(queryData.getLimit());
            }
            
            if (queryData.getAllowFiltering() != null) {
                if (queryData.getAllowFiltering()) {
                    select.allowFiltering();
                }
            }
            
            if (queryData.getFetchSize() != null) {
                select.setFetchSize(queryData.getFetchSize());
            }
            
            return select;
        }


        @Override
        public Count execute() {
            return ListenableFutures.getUninterruptibly(executeAsync());
        }      
        
        
        @Override
        public CompletableFuture<Count> executeAsync() {
            return performAsync(getDefaultDbSession(), toStatement(data)).thenApply(resultSet -> Count.newCountResult(resultSet));
        }
        
        @Override
        public Publisher<Count> executeRx() {
            return executeAsync().thenApply(count -> new SingleEntryResultList(count));
        }

		@Override
		public ListRead<Count, Count> withPagingState(PagingState pagingState) {
			throw new IllegalArgumentException("Count readers cannot be configured with paging state.");
		}
    }  
}
    