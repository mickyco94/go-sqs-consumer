package sqs

import (
	"fmt"
)

type MessageStateError struct {
	CurrentState HandlerResult
	Next         HandlerResult
}

func (e *MessageStateError) Error() string {
	return fmt.Sprintf("Attempted to transition to %v when current handler result is %v",
		e.CurrentState,
		e.Next)
}

type requestHandler struct {
	request Request
	q       *queue
	result  HandlerResult
}

//DeadLetter directly does this, instead we should write to a channel that is consumed within this file
//That dead-letters the queue itself
func (r *requestHandler) DeadLetter() error {

	if r.result != Unhandled {
		return &MessageStateError{
			CurrentState: r.result,
			Next:         DeadLetter,
		}
	}

	r.result = DeadLetter
	func() {
		r.q.deadLetterChannel <- &r.request.originalMessage
	}()

	return nil
}

func (r *requestHandler) Retry() error {

	if r.result != Unhandled {
		return &MessageStateError{
			CurrentState: r.result,
			Next:         Retry,
		}
	}

	//Determine if we should retry or DL here?

	r.result = Retry
	go func() {
		r.q.retryChannel <- &r.request.originalMessage
	}()

	return nil
}

func (r *requestHandler) Handled() error {

	if r.result != Unhandled {
		return &MessageStateError{
			CurrentState: r.result,
			Next:         Handled,
		}
	}

	r.result = Handled

	go func() {
		r.q.handleChannel <- receiptHandle(r.request.originalMessage.ReceiptHandle)
	}()

	return nil
}

func (r *requestHandler) GetResult() HandlerResult {
	return r.result
}
