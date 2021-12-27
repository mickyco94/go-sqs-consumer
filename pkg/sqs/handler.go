package sqs

import (
	"fmt"
)

type MessageStateError struct {
	Next string
}

func (e *MessageStateError) Error() string {
	return fmt.Sprintf("Attempted to modify state of method that has already been completed",
		e.Next)
}

//Possibly instead of actually acting on the dead letter in this method
//We could instead store the result and have a private "complete" method that actually acts on it..?
type requestHandler struct {
	request Request
	q       *queue
	acted   bool
}

//DeadLetter directly does this, instead we should write to a channel that is consumed within this file
//That dead-letters the queue itself
func (r *requestHandler) DeadLetter() error {

	if r.acted {
		return &MessageStateError{
			Next: "DeadLetter",
		}
	}

	// r.result = DeadLetter
	r.acted = true

	err := r.q.deleteMessage(r.request.originalMessage.ReceiptHandle)

	if err != nil {
		return err
	}

	return nil
}

func (r *requestHandler) Retry() error {

	if r.acted {
		return &MessageStateError{
			Next: "DeadLetter",
		}
	}

	// r.result = Retry

	//Return an error from this
	r.q.retry(&r.request.originalMessage)

	return nil
}

func (r *requestHandler) Handled() error {

	if r.acted {
		return &MessageStateError{
			Next: "DeadLetter",
		}
	}

	r.acted = true

	err := r.q.deleteMessage(r.request.originalMessage.ReceiptHandle)

	if err != nil {
		return err
	}

	return nil
}

// func (r *requestHandler) GetResult() HandlerResult {
// 	return r.result
// }
