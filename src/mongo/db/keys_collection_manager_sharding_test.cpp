/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/jsobj.h"
#include "mongo/db/keys_collection_client_sharded.h"
#include "mongo/db/keys_collection_document_gen.h"
#include "mongo/db/keys_collection_manager.h"
#include "mongo/db/s/config/config_server_test_fixture.h"
#include "mongo/db/time_proof_service.h"
#include "mongo/db/vector_clock_mutable.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/clock_source_mock.h"
#include "mongo/util/fail_point.h"

namespace mongo {
namespace {

class KeysManagerShardedTest : public ConfigServerTestFixture {
public:
    KeysCollectionManager* keyManager() {
        return _keyManager.get();
    }

protected:
    void setUp() override {
        ConfigServerTestFixture::setUp();

        auto clockSource = std::make_unique<ClockSourceMock>();
        // Timestamps of "0 seconds" are not allowed, so we must advance our clock mock to the first
        // real second.
        clockSource->advance(Seconds(1));

        operationContext()->getServiceContext()->setFastClockSource(std::move(clockSource));
        auto catalogClient = std::make_unique<KeysCollectionClientSharded>(
            Grid::get(operationContext())->catalogClient());
        _keyManager =
            std::make_unique<KeysCollectionManager>("dummy", std::move(catalogClient), Seconds(1));
    }

    void tearDown() override {
        _keyManager->stopMonitoring();

        ConfigServerTestFixture::tearDown();
    }

private:
    std::unique_ptr<KeysCollectionManager> _keyManager;
};

TEST_F(KeysManagerShardedTest, GetKeyForValidationTimesOutIfRefresherIsNotRunning) {
    operationContext()->setDeadlineAfterNowBy(Microseconds(250 * 1000),
                                              ErrorCodes::ExceededTimeLimit);

    ASSERT_THROWS(
        keyManager()->getKeyForValidation(operationContext(), 1, LogicalTime(Timestamp(100, 0))),
        DBException);
}

TEST_F(KeysManagerShardedTest, GetKeyForValidationErrorsIfKeyDoesntExist) {
    keyManager()->startMonitoring(getServiceContext());

    auto keyStatus =
        keyManager()->getKeyForValidation(operationContext(), 1, LogicalTime(Timestamp(100, 0)));
    ASSERT_EQ(ErrorCodes::KeyNotFound, keyStatus.getStatus());
}

TEST_F(KeysManagerShardedTest, GetKeyWithSingleKey) {
    keyManager()->startMonitoring(getServiceContext());

    KeysCollectionDocument origKey1(
        1, "dummy", TimeProofService::generateRandomKey(), LogicalTime(Timestamp(105, 0)));
    ASSERT_OK(insertToConfigCollection(
        operationContext(), NamespaceString::kKeysCollectionNamespace, origKey1.toBSON()));

    auto keyStatus =
        keyManager()->getKeyForValidation(operationContext(), 1, LogicalTime(Timestamp(100, 0)));
    ASSERT_OK(keyStatus.getStatus());

    auto key = keyStatus.getValue();
    ASSERT_EQ(1, key.getKeyId());
    ASSERT_EQ(origKey1.getKey(), key.getKey());
    ASSERT_EQ(Timestamp(105, 0), key.getExpiresAt().asTimestamp());
}

TEST_F(KeysManagerShardedTest, GetKeyWithMultipleKeys) {
    keyManager()->startMonitoring(getServiceContext());

    KeysCollectionDocument origKey1(
        1, "dummy", TimeProofService::generateRandomKey(), LogicalTime(Timestamp(105, 0)));
    ASSERT_OK(insertToConfigCollection(
        operationContext(), NamespaceString::kKeysCollectionNamespace, origKey1.toBSON()));

    KeysCollectionDocument origKey2(
        2, "dummy", TimeProofService::generateRandomKey(), LogicalTime(Timestamp(205, 0)));
    ASSERT_OK(insertToConfigCollection(
        operationContext(), NamespaceString::kKeysCollectionNamespace, origKey2.toBSON()));

    auto keyStatus =
        keyManager()->getKeyForValidation(operationContext(), 1, LogicalTime(Timestamp(100, 0)));
    ASSERT_OK(keyStatus.getStatus());

    auto key = keyStatus.getValue();
    ASSERT_EQ(1, key.getKeyId());
    ASSERT_EQ(origKey1.getKey(), key.getKey());
    ASSERT_EQ(Timestamp(105, 0), key.getExpiresAt().asTimestamp());

    keyStatus =
        keyManager()->getKeyForValidation(operationContext(), 2, LogicalTime(Timestamp(100, 0)));
    ASSERT_OK(keyStatus.getStatus());

    key = keyStatus.getValue();
    ASSERT_EQ(2, key.getKeyId());
    ASSERT_EQ(origKey2.getKey(), key.getKey());
    ASSERT_EQ(Timestamp(205, 0), key.getExpiresAt().asTimestamp());
}

TEST_F(KeysManagerShardedTest, GetKeyShouldErrorIfKeyIdMismatchKey) {
    keyManager()->startMonitoring(getServiceContext());

    KeysCollectionDocument origKey1(
        1, "dummy", TimeProofService::generateRandomKey(), LogicalTime(Timestamp(105, 0)));
    ASSERT_OK(insertToConfigCollection(
        operationContext(), NamespaceString::kKeysCollectionNamespace, origKey1.toBSON()));

    auto keyStatus =
        keyManager()->getKeyForValidation(operationContext(), 2, LogicalTime(Timestamp(100, 0)));
    ASSERT_EQ(ErrorCodes::KeyNotFound, keyStatus.getStatus());
}

TEST_F(KeysManagerShardedTest, GetKeyWithoutRefreshShouldReturnRightKey) {
    keyManager()->startMonitoring(getServiceContext());

    KeysCollectionDocument origKey1(
        1, "dummy", TimeProofService::generateRandomKey(), LogicalTime(Timestamp(105, 0)));
    ASSERT_OK(insertToConfigCollection(
        operationContext(), NamespaceString::kKeysCollectionNamespace, origKey1.toBSON()));
    KeysCollectionDocument origKey2(
        2, "dummy", TimeProofService::generateRandomKey(), LogicalTime(Timestamp(110, 0)));
    ASSERT_OK(insertToConfigCollection(
        operationContext(), NamespaceString::kKeysCollectionNamespace, origKey2.toBSON()));

    {
        auto keyStatus = keyManager()->getKeyForValidation(
            operationContext(), 1, LogicalTime(Timestamp(100, 0)));
        ASSERT_OK(keyStatus.getStatus());

        auto key = keyStatus.getValue();
        ASSERT_EQ(1, key.getKeyId());
        ASSERT_EQ(origKey1.getKey(), key.getKey());
        ASSERT_EQ(Timestamp(105, 0), key.getExpiresAt().asTimestamp());
    }

    {
        auto keyStatus = keyManager()->getKeyForValidation(
            operationContext(), 2, LogicalTime(Timestamp(105, 0)));
        ASSERT_OK(keyStatus.getStatus());

        auto key = keyStatus.getValue();
        ASSERT_EQ(2, key.getKeyId());
        ASSERT_EQ(origKey2.getKey(), key.getKey());
        ASSERT_EQ(Timestamp(110, 0), key.getExpiresAt().asTimestamp());
    }
}

TEST_F(KeysManagerShardedTest, GetKeyForSigningShouldReturnRightKey) {
    keyManager()->startMonitoring(getServiceContext());

    KeysCollectionDocument origKey1(
        1, "dummy", TimeProofService::generateRandomKey(), LogicalTime(Timestamp(105, 0)));
    ASSERT_OK(insertToConfigCollection(
        operationContext(), NamespaceString::kKeysCollectionNamespace, origKey1.toBSON()));

    keyManager()->refreshNow(operationContext());

    auto keyStatus = keyManager()->getKeyForSigning(nullptr, LogicalTime(Timestamp(100, 0)));
    ASSERT_OK(keyStatus.getStatus());

    auto key = keyStatus.getValue();
    ASSERT_EQ(1, key.getKeyId());
    ASSERT_EQ(origKey1.getKey(), key.getKey());
    ASSERT_EQ(Timestamp(105, 0), key.getExpiresAt().asTimestamp());
}

TEST_F(KeysManagerShardedTest, GetKeyForSigningShouldReturnRightOldKey) {
    keyManager()->startMonitoring(getServiceContext());

    KeysCollectionDocument origKey1(
        1, "dummy", TimeProofService::generateRandomKey(), LogicalTime(Timestamp(105, 0)));
    ASSERT_OK(insertToConfigCollection(
        operationContext(), NamespaceString::kKeysCollectionNamespace, origKey1.toBSON()));
    KeysCollectionDocument origKey2(
        2, "dummy", TimeProofService::generateRandomKey(), LogicalTime(Timestamp(110, 0)));
    ASSERT_OK(insertToConfigCollection(
        operationContext(), NamespaceString::kKeysCollectionNamespace, origKey2.toBSON()));

    keyManager()->refreshNow(operationContext());

    {
        auto keyStatus = keyManager()->getKeyForSigning(nullptr, LogicalTime(Timestamp(100, 0)));
        ASSERT_OK(keyStatus.getStatus());

        auto key = keyStatus.getValue();
        ASSERT_EQ(1, key.getKeyId());
        ASSERT_EQ(origKey1.getKey(), key.getKey());
        ASSERT_EQ(Timestamp(105, 0), key.getExpiresAt().asTimestamp());
    }

    {
        auto keyStatus = keyManager()->getKeyForSigning(nullptr, LogicalTime(Timestamp(105, 0)));
        ASSERT_OK(keyStatus.getStatus());

        auto key = keyStatus.getValue();
        ASSERT_EQ(2, key.getKeyId());
        ASSERT_EQ(origKey2.getKey(), key.getKey());
        ASSERT_EQ(Timestamp(110, 0), key.getExpiresAt().asTimestamp());
    }
}

TEST_F(KeysManagerShardedTest, ShouldCreateKeysIfKeyGeneratorEnabled) {
    keyManager()->startMonitoring(getServiceContext());

    const LogicalTime currentTime(LogicalTime(Timestamp(100, 0)));
    VectorClockMutable::get(operationContext())->tickClusterTimeTo(currentTime);

    keyManager()->enableKeyGenerator(operationContext(), true);
    keyManager()->refreshNow(operationContext());

    auto keyStatus = keyManager()->getKeyForSigning(nullptr, LogicalTime(Timestamp(100, 100)));
    ASSERT_OK(keyStatus.getStatus());

    auto key = keyStatus.getValue();
    ASSERT_EQ(Timestamp(101, 0), key.getExpiresAt().asTimestamp());
}

TEST_F(KeysManagerShardedTest, EnableModeFlipFlopStressTest) {
    keyManager()->startMonitoring(getServiceContext());

    const LogicalTime currentTime(LogicalTime(Timestamp(100, 0)));
    VectorClockMutable::get(operationContext())->tickClusterTimeTo(currentTime);

    bool doEnable = true;

    for (int x = 0; x < 10; x++) {
        keyManager()->enableKeyGenerator(operationContext(), doEnable);
        keyManager()->refreshNow(operationContext());

        auto keyStatus = keyManager()->getKeyForSigning(nullptr, LogicalTime(Timestamp(100, 100)));
        ASSERT_OK(keyStatus.getStatus());

        auto key = keyStatus.getValue();
        ASSERT_EQ(Timestamp(101, 0), key.getExpiresAt().asTimestamp());

        doEnable = !doEnable;
    }
}

TEST_F(KeysManagerShardedTest, ShouldStillBeAbleToUpdateCacheEvenIfItCantCreateKeys) {
    KeysCollectionDocument origKey1(
        1, "dummy", TimeProofService::generateRandomKey(), LogicalTime(Timestamp(105, 0)));
    ASSERT_OK(insertToConfigCollection(
        operationContext(), NamespaceString::kKeysCollectionNamespace, origKey1.toBSON()));

    // Set the time to be very ahead so the updater will be forced to create new keys.
    const LogicalTime fakeTime(Timestamp(20000, 0));
    VectorClockMutable::get(operationContext())->tickClusterTimeTo(fakeTime);

    FailPointEnableBlock failWriteBlock("failCollectionInserts");

    {
        FailPointEnableBlock failQueryBlock("planExecutorAlwaysFails");
        keyManager()->startMonitoring(getServiceContext());
        keyManager()->enableKeyGenerator(operationContext(), true);
    }

    auto keyStatus =
        keyManager()->getKeyForValidation(operationContext(), 1, LogicalTime(Timestamp(100, 0)));
    ASSERT_OK(keyStatus.getStatus());

    auto key = keyStatus.getValue();
    ASSERT_EQ(1, key.getKeyId());
    ASSERT_EQ(origKey1.getKey(), key.getKey());
    ASSERT_EQ(Timestamp(105, 0), key.getExpiresAt().asTimestamp());
}

TEST_F(KeysManagerShardedTest, ShouldNotCreateKeysWithDisableKeyGenerationFailPoint) {
    const LogicalTime currentTime(Timestamp(100, 0));
    VectorClockMutable::get(operationContext())->tickClusterTimeTo(currentTime);

    {
        FailPointEnableBlock failKeyGenerationBlock("disableKeyGeneration");
        keyManager()->startMonitoring(getServiceContext());
        keyManager()->enableKeyGenerator(operationContext(), true);

        keyManager()->refreshNow(operationContext());
        auto keyStatus = keyManager()->getKeyForValidation(
            operationContext(), 1, LogicalTime(Timestamp(100, 0)));
        ASSERT_EQ(ErrorCodes::KeyNotFound, keyStatus.getStatus());
    }

    // Once the failpoint is disabled, the generator can make keys again.
    keyManager()->refreshNow(operationContext());
    auto keyStatus = keyManager()->getKeyForSigning(nullptr, LogicalTime(Timestamp(100, 0)));
    ASSERT_OK(keyStatus.getStatus());
}

TEST_F(KeysManagerShardedTest, HasSeenKeysIsFalseUntilKeysAreFound) {
    const LogicalTime currentTime(Timestamp(100, 0));
    VectorClockMutable::get(operationContext())->tickClusterTimeTo(currentTime);

    ASSERT_EQ(false, keyManager()->hasSeenKeys());

    {
        FailPointEnableBlock failKeyGenerationBlock("disableKeyGeneration");
        keyManager()->startMonitoring(getServiceContext());
        keyManager()->enableKeyGenerator(operationContext(), true);

        keyManager()->refreshNow(operationContext());
        auto keyStatus = keyManager()->getKeyForValidation(
            operationContext(), 1, LogicalTime(Timestamp(100, 0)));
        ASSERT_EQ(ErrorCodes::KeyNotFound, keyStatus.getStatus());

        ASSERT_EQ(false, keyManager()->hasSeenKeys());
    }

    // Once the failpoint is disabled, the generator can make keys again.
    keyManager()->refreshNow(operationContext());
    auto keyStatus = keyManager()->getKeyForSigning(nullptr, LogicalTime(Timestamp(100, 0)));
    ASSERT_OK(keyStatus.getStatus());

    ASSERT_EQ(true, keyManager()->hasSeenKeys());
}

LogicalTime addSeconds(const LogicalTime& logicalTime, const Seconds& seconds) {
    auto asTimestamp = logicalTime.asTimestamp();
    return LogicalTime(Timestamp(asTimestamp.getSecs() + seconds.count(), asTimestamp.getInc()));
}

TEST(KeysCollectionManagerUtilTest, HowMuchSleepNeedForWithDefaultKeysRotationIntervalIs20Days) {
    auto secondsSinceEpoch = durationCount<Seconds>(Date_t::now().toDurationSinceEpoch());
    auto defaultKeysIntervalSeconds = Seconds(KeysRotationIntervalSec);

    auto currentTime = LogicalTime(Timestamp(secondsSinceEpoch, 0));
    auto latestExpiredAt = addSeconds(currentTime, defaultKeysIntervalSeconds * 2);
    auto defaultInterval = Milliseconds(defaultKeysIntervalSeconds);

    auto nextWakeupMillis = keys_collection_manager_util::howMuchSleepNeedFor(
        currentTime, latestExpiredAt, defaultInterval);
    ASSERT_EQ(nextWakeupMillis, Days(20));
}

TEST(KeysCollectionManagerUtilTest, HowMuchSleepNeedForIsNeverLongerThan20Days) {
    auto secondsSinceEpoch = durationCount<Seconds>(Date_t::now().toDurationSinceEpoch());
    auto keysRotationInterval = Seconds(Days(50));

    auto currentTime = LogicalTime(Timestamp(secondsSinceEpoch, 0));
    auto latestExpiredAt = addSeconds(currentTime, keysRotationInterval * 2);
    auto interval = Milliseconds(keysRotationInterval);

    auto nextWakeupMillis =
        keys_collection_manager_util::howMuchSleepNeedFor(currentTime, latestExpiredAt, interval);
    ASSERT_EQ(nextWakeupMillis, Days(20));
}

TEST(KeysCollectionManagerUtilTest, HowMuchSleepNeedForIsNeverHigherThanRotationInterval) {
    auto secondsSinceEpoch = durationCount<Seconds>(Date_t::now().toDurationSinceEpoch());
    auto keysRotationInterval = Seconds(Days(5));

    auto currentTime = LogicalTime(Timestamp(secondsSinceEpoch, 0));
    auto latestExpiredAt = addSeconds(currentTime, keysRotationInterval * 2);
    auto interval = Milliseconds(keysRotationInterval);

    auto nextWakeupMillis =
        keys_collection_manager_util::howMuchSleepNeedFor(currentTime, latestExpiredAt, interval);
    ASSERT_EQ(nextWakeupMillis, interval);
}

LogicalTime subtractSeconds(const LogicalTime& logicalTime, const Seconds& seconds) {
    auto asTimestamp = logicalTime.asTimestamp();
    return LogicalTime(Timestamp(asTimestamp.getSecs() - seconds.count(), asTimestamp.getInc()));
}

TEST(KeysCollectionManagerUtilTest, HowMuchSleepNeedForAfterNotFindingKeys) {
    // Default refresh interval if keys could not be found.
    const Milliseconds kRefreshIntervalIfErrored(200);

    auto secondsSinceEpoch = durationCount<Seconds>(Date_t::now().toDurationSinceEpoch());
    auto keysRotationInterval = Milliseconds(5000);

    // The latest found key expired before the current time, which means no new keys were found
    // despite the previous refresh succeeding.
    auto currentTime = LogicalTime(Timestamp(secondsSinceEpoch, 0));
    auto latestExpiredAt = subtractSeconds(currentTime, Seconds(1));
    auto interval = Milliseconds(keysRotationInterval);

    auto nextWakeupMillis =
        keys_collection_manager_util::howMuchSleepNeedFor(currentTime, latestExpiredAt, interval);
    ASSERT_EQ(nextWakeupMillis, kRefreshIntervalIfErrored);
}

}  // namespace
}  // namespace mongo
