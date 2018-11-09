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

void
BucketApplicator::advance()
{
    auto &session = mDb.getSession();
    soci::transaction sqlTx(session);

    // This maps a query to N vectors.
    // Each vector records successive values for a specific query parameter.
    std::map<std::string, UseVecVec> queries_and_args;

    while (mBucketIter)
    {
        LedgerHeader lh;
        LedgerDelta delta(lh, mDb, false);

        QueryAndArgs qa;

        auto const& entry = *mBucketIter;
        if (entry.type() == LIVEENTRY)
        {
            EntryFrame::pointer ep = EntryFrame::FromXDR(entry.liveEntry());
            ep->storeAddOrChange(delta, mDb, qa);
        }
        else
        {
            EntryFrame::storeDelete(delta, mDb, entry.deadEntry(), qa);
        }

        UseVecVec& argses = queries_and_args[qa.first];
        if (argses.size() == 0) {
          for (auto const& a: qa.second) {
            UseVec v;
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

        ++mBucketIter;
        // No-op, just to avoid needless rollback.
        delta.commit();
        if ((++mSize & 0xff) == 0xff)
        {
            break;
        }
    }

    // We collected the queries and their arguments above.
    // Now execute them.

    for (auto const& it: queries_and_args) {
      std::string const& query = it.first;
      UseVecVec const& args = it.second;

      soci::statement st(session);
      for (UseVec const& a: args) {
        st.exchange(a);
      }

      st.alloc();
      st.prepare(query);
      st.define_and_bind();
      st.execute(true);
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
