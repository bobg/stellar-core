#pragma once

// Copyright 2014 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "bucket/LedgerCmp.h"
#include "overlay/StellarXDR.h"
#include "util/NonCopyable.h"
#include <soci.h>

/*
Frame
Parent of AccountFrame, TrustFrame, OfferFrame

These just hold the xdr LedgerEntry objects and have some associated functions
*/

namespace stellar
{
class Database;
class LedgerDelta;

// A vector of soci use(...) values.
typedef std::vector<soci::details::use_type_ptr> UseVec;

// A vector of UseVecs.
typedef std::vector<UseVec> UseVecVec;

// A SQL query in string form and the values to bind to it.
typedef std::pair<std::string, UseVec> QueryAndArgs;

class EntryFrame : public NonMovableOrCopyable
{
  protected:
    mutable bool mKeyCalculated;
    mutable LedgerKey mKey;
    void
    clearCached()
    {
        mKeyCalculated = false;
    }

  public:
    typedef std::shared_ptr<EntryFrame> pointer;

    LedgerEntry mEntry;

    EntryFrame(LedgerEntryType type);
    EntryFrame(LedgerEntry const& from);
    virtual ~EntryFrame() = default;

    static pointer FromXDR(LedgerEntry const& from);
    static pointer storeLoad(LedgerKey const& key, Database& db);

    // Static helpers for working with the DB LedgerEntry cache.
    static void flushCachedEntry(LedgerKey const& key, Database& db);
    static bool cachedEntryExists(LedgerKey const& key, Database& db);
    static std::shared_ptr<LedgerEntry const>
    getCachedEntry(LedgerKey const& key, Database& db);
    static void putCachedEntry(LedgerKey const& key,
                               std::shared_ptr<LedgerEntry const> p,
                               Database& db);

    // helpers to get/set the last modified field
    uint32 getLastModified() const;
    uint32& getLastModified();
    void touch(uint32 ledgerSeq);

    // touch the entry if the delta is tracking a ledger header with
    // a sequence that is not 0 (0 is used when importing buckets)
    void touch(LedgerDelta const& delta);

    // Member helpers that call cache flush/put for self.
    void flushCachedEntry(Database& db) const;
    void putCachedEntry(Database& db) const;

    static std::string checkAgainstDatabase(LedgerEntry const& entry,
                                            Database& db);

    virtual EntryFrame::pointer copy() const = 0;

    LedgerKey const& getKey() const;

    void storeDelete(LedgerDelta& delta, Database& db) const;
    virtual void storeDelete(LedgerDelta& delta, Database& db, QueryAndArgs& qa) const = 0;

    // change/add may update the entry (last modified)

    void storeChange(LedgerDelta& delta, Database& db);
    virtual void storeChange(LedgerDelta& delta, Database& db, QueryAndArgs& qa) = 0;

    void storeAdd(LedgerDelta& delta, Database& db);
    virtual void storeAdd(LedgerDelta& delta, Database& db, QueryAndArgs& qa) = 0;

    void storeAddOrChange(LedgerDelta& delta, Database& db);
    void storeAddOrChange(LedgerDelta& delta, Database& db, QueryAndArgs& qa);

    static bool exists(Database& db, LedgerKey const& key);
    static void storeDelete(LedgerDelta& delta, Database& db,
                            LedgerKey const& key);
    static void storeDelete(LedgerDelta& delta, Database& db,
                            LedgerKey const& key, QueryAndArgs& qa);
};

// static helper for getting a LedgerKey from a LedgerEntry.
LedgerKey LedgerEntryKey(LedgerEntry const& e);
}
