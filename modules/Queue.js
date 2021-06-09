// A FIFO stack-like data structure

class Queue {

    constructor() {
        this.clear() // Initialize the stack
    }

    push(element) {
        // Push a contact onto the stack
        this.stack.push(element)
    }

    pop() {
        // Remove and return the topmost contact
        if( this.stack.length == 0 )
            return undefined
        return this.stack.splice(0,1)[0] // Remove and return the first element
    }

    length() {
        return this.stack.length
    }

    isEmpty() {
        return this.stack.length == 0;
    }

    // Empty the stack
    clear() {
        this.stack = []
    }
}

module.exports = Queue