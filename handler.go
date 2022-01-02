package sqs

import (
	"errors"
	"fmt"
)

type MessageStateError struct {
	Current MessageState
	Next    MessageState
}

func (e *MessageStateError) Error() string {
	return fmt.Sprintf("Attempted to change message state to %v when current state is %v",
		e.Next,
		e.Current)
}

//Possibly instead of actually acting on the dead letter in this method
//We could instead store the result and have a private "complete" method that actually acts on it..?
type requestHandler struct {
	request Request
	q       *queue
	state   MessageState
}

//DeadLetter directly does this, instead we should write to a channel that is consumed within this file
//That dead-letters the queue itself
func (r *requestHandler) DeadLetter() error {

	if r.state != Unhandled {
		return &MessageStateError{
			Current: r.state,
			Next:    DeadLetter,
		}
	}

	err := r.q.deleteMessage(r.request.originalMessage.ReceiptHandle)

	if err != nil {
		return err
	}

	r.state = DeadLetter

	return nil
}

func (r *requestHandler) Retry() error {

	if r.state != Unhandled {
		return &MessageStateError{
			Current: r.state,
			Next:    Retry,
		}
	}

	if r.request.Attempt >= r.request.MaxAttempts {
		return errors.New("exceeded retry attempts")
	}

	//Return an error from this
	r.q.retry(&r.request.originalMessage)

	r.state = Retry

	return nil
}

func (r *requestHandler) Handled() error {

	if r.state != Unhandled {
		return &MessageStateError{
			Current: r.state,
			Next:    Retry,
		}
	}

	err := r.q.deleteMessage(r.request.originalMessage.ReceiptHandle)

	if err != nil {
		return err
	}

	r.state = Handled

	return nil
}

func (r *requestHandler) GetResult() MessageState {
	return r.state
}
