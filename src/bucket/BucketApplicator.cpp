// Copyright 2015 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "util/asio.h"
#include "bucket/BucketApplicator.h"
#include "bucket/Bucket.h"
#include "ledger/LedgerDelta.h"
#include "util/Logging.h"

namespace stellar
{

BucketApplicator::BucketApplicator(Database& db,
                                   std::shared_ptr<const Bucket> bucket)
    : mDb(db), mBucketIter(bucket)
{
}

BucketApplicator::operator bool() const
{
    return (bool)mBucketIter;
}

typedef std::vector<soci::details::use_type_ptr> use_vec;
typedef std::vector<use_vec> use_vec_vec;
typedef std::pair<std::string, use_vec> query_and_args;

void
BucketApplicator::advance()
{
    soci::transaction sqlTx(mDb.getSession());
    std::map<std::string, std::vector<use_vec>> queries_and_args;
    while (mBucketIter)
    {
        LedgerHeader lh;
        LedgerDelta delta(lh, mDb, false);

        auto const& entry = *mBucketIter;
        if (entry.type() == LIVEENTRY)
        {
            EntryFrame::pointer ep = EntryFrame::FromXDR(entry.liveEntry());
            query_and_args qa = ep->qaStoreAddOrChange(delta, mDb);
            // qa is now pair<QUERY, vector<ARG1, ARG2, ..., ARGn>>
            use_vec_vec& argses = queries_and_args[qa.first];
            if (argses.size() == 0) {
              for (auto const& a: qa.second) {
                use_vec v;
                v.push_back(a);
                argses.push_back(v);
              }
            } else if (argses.size() != qa.second.size()) {
              // xxx error
            } else {
              for (int i = 0; i < argses.size(); ++i) {
                argses[i].push_back(qa.second[i])
              }
            }
        }
        else
        {
          query_and_args qa = EntryFrame::qaStoreDelete(delta, mDb, entry.deadEntry());
          use_vec_vec& argses = queries_and_args[qa.first];
          if (argses.size() == 0) {
            for (auto const& a: qa.second) {
              use_vec v;
              v.push_back(a);
              argses.push_back(v);
            }
          } else if (argses.size() != qa.second.size()) {
            // xxx error
          } else {
            for (int i = 0; i < argses.size(); ++i) {
              argses[i].push_back(qa.second[i]);
            }
          }
        }
        ++mBucketIter;
        // No-op, just to avoid needless rollback.
        delta.commit();
        if ((++mSize & 0xff) == 0xff)
        {
            break;
        }
    }

    


    sqlTx.commit();
    mDb.clearPreparedStatementCache();

    if (!mBucketIter || (mSize & 0xfff) == 0xfff)
    {
        CLOG(INFO, "Bucket")
            << "Bucket-apply: committed " << mSize << " entries";
    }
}
}
