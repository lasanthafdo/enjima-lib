//
// Created by m34ferna on 09/01/24.
//

#include "StreamingOperator.h"

#include <iostream>


namespace enjima::operators {

    StreamingOperator::StreamingOperator(OperatorID opId, std::string opName)
        : operatorId_(opId), operatorName_(std::move(opName))
    {
    }

    StreamingOperator::~StreamingOperator() = default;

    OperatorID StreamingOperator::GetOperatorId() const
    {
        return operatorId_;
    }

    bool StreamingOperator::IsSourceOperator() const
    {
        return false;
    }

    bool StreamingOperator::IsSinkOperator() const
    {
        return false;
    }

    const std::string& StreamingOperator::GetOperatorName() const
    {
        return operatorName_;
    }

}// namespace enjima::operators