/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <BufferManagerListener.hpp>
#include <Runtime/BufferManager.hpp>
#include <Thread.hpp>
#include <iostream>
#include <fmt/format.h>

namespace NES
{

namespace
{
void threadRoutine(const std::stop_token& token, std::shared_ptr<BufferManager> bufferManager)
{
    Thread::setThreadName("BufferManagerListener");

    while (!token.stop_requested())
    {
        // Wait 200ms or until stop requested
        auto sleepTime = std::chrono::milliseconds(100);
        auto now = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - now < sleepTime && !token.stop_requested()) {
             std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        if (token.stop_requested())
        {
             break;
        }

        if (!bufferManager)
        {
            continue;
        }

        const auto numOfPooledBuffers = bufferManager->getNumOfPooledBuffers();
        const auto availableBuffers = bufferManager->getNumberOfAvailableBuffers();
        const auto usedBuffers = numOfPooledBuffers - availableBuffers;
        const auto numOfUnpooledBuffers = bufferManager->getNumOfUnpooledBuffers();

        std::cout << fmt::format("BufferManager is currently using {} out of {} buffers, unpooled: {}\n",
                                 usedBuffers, numOfPooledBuffers, numOfUnpooledBuffers);
    }
}
}

BufferManagerListener::BufferManagerListener(std::shared_ptr<BufferManager> bufferManager)
    : bufferManager(std::move(bufferManager))
    , calculateThread([this](const std::stop_token& stopToken)
                      { threadRoutine(stopToken, this->bufferManager); })
{
}

BufferManagerListener::~BufferManagerListener() = default;

void BufferManagerListener::onEvent(Event)
{
    // Do nothing as this listener is not event-driven
}

} // namespace NES
