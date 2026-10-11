package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type User struct {
	ID       int
	Username string
}

type Message struct {
	ID           int
	SenderID     int
	SenderName   string
	ReceiverID   int
	ReceiverName string
	Content      string
	Timestamp    time.Time
}

type CommandType int

const (
	SendMessage CommandType = iota
	GetHistory
	FilterUser
	SearchKeyword
	Shutdown
)

type Command struct {
	Type     CommandType
	Message  Message
	UserID   int
	Keyword  string
	Response chan []Message
}

type ChatServer struct {
	history []Message
	mu      sync.RWMutex
}

func NewChatServer() *ChatServer {
	return &ChatServer{
		history: make([]Message, 0),
	}
}

func (server *ChatServer) run(commands <-chan Command, done chan<- bool) {

	for command := range commands {

		switch command.Type {

		case SendMessage:

			server.mu.Lock()

			server.history = append(
				server.history,
				command.Message,
			)

			server.mu.Unlock()

			fmt.Printf(
				"\n[SERVER] Message received: %s -> %s\n",
				command.Message.SenderName,
				command.Message.ReceiverName,
			)

			fmt.Printf(
				"[SERVER] %s: %s\n",
				command.Message.SenderName,
				command.Message.Content,
			)

		case GetHistory:

			server.mu.RLock()

			historyCopy := make(
				[]Message,
				len(server.history),
			)

			copy(historyCopy, server.history)

			server.mu.RUnlock()

			command.Response <- historyCopy

		case FilterUser:

			server.mu.RLock()

			results := make([]Message, 0)

			for _, message := range server.history {

				if message.SenderID == command.UserID ||
					message.ReceiverID == command.UserID {

					results = append(results, message)
				}
			}

			server.mu.RUnlock()

			command.Response <- results

		case SearchKeyword:

			server.mu.RLock()

			results := make([]Message, 0)

			keyword := strings.ToLower(
				command.Keyword,
			)

			for _, message := range server.history {

				content := strings.ToLower(
					message.Content,
				)

				if strings.Contains(content, keyword) {
					results = append(results, message)
				}
			}

			server.mu.RUnlock()

			command.Response <- results

		case Shutdown:

			fmt.Println(
				"\n[SERVER] Chat server shutting down...",
			)

			done <- true
			return
		}
	}
}

func readInput(reader *bufio.Reader, prompt string) string {

	fmt.Print(prompt)

	input, err := reader.ReadString('\n')

	if err != nil {
		return ""
	}

	return strings.TrimSpace(input)
}

func displayUsers(users []User) {

	fmt.Println("\n========== USERS ==========")

	for _, user := range users {

		fmt.Printf(
			"ID: %d | Username: %s\n",
			user.ID,
			user.Username,
		)
	}

	fmt.Println("===========================")
}

func findUser(users []User, id int) *User {

	for _, user := range users {

		if user.ID == id {
			return &user
		}
	}

	return nil
}

func displayMessages(messages []Message) {

	if len(messages) == 0 {
		fmt.Println("\nNo messages found.")
		return
	}

	fmt.Println(
		"\n=============== MESSAGE HISTORY ===============",
	)

	for _, message := range messages {

		fmt.Printf(
			"[%s] #%d %s (%d) -> %s (%d): %s\n",

			message.Timestamp.Format(
				"2006-01-02 15:04:05",
			),

			message.ID,

			message.SenderName,
			message.SenderID,

			message.ReceiverName,
			message.ReceiverID,

			message.Content,
		)
	}

	fmt.Println(
		"================================================",
	)
}

func sendMessage(
	commands chan<- Command,
	sender User,
	receiver User,
	content string,
	messageID int,
) {

	message := Message{

		ID:           messageID,
		SenderID:     sender.ID,
		SenderName:   sender.Username,
		ReceiverID:   receiver.ID,
		ReceiverName: receiver.Username,

		Content: content,

		Timestamp: time.Now(),
	}

	commands <- Command{
		Type:    SendMessage,
		Message: message,
	}

	fmt.Println(
		"Message sent successfully.",
	)
}

func main() {

	fmt.Println(
		"==========================================",
	)

	fmt.Println(
		"          GO TEXT-BASED CHAT APP",
	)

	fmt.Println(
		"==========================================",
	)

	users := []User{

		{
			ID:       1,
			Username: "Alice",
		},

		{
			ID:       2,
			Username: "Bob",
		},

		{
			ID:       3,
			Username: "Charlie",
		},
	}

	server := NewChatServer()

	commands := make(chan Command)

	done := make(chan bool)

	/*
		The server executes concurrently
		in its own goroutine.
	*/

	go server.run(commands, done)

	reader := bufio.NewReader(os.Stdin)

	messageID := 1

	for {

		fmt.Println("\n========== CHAT MENU ==========")

		fmt.Println("1. Display users")
		fmt.Println("2. Send message")
		fmt.Println("3. Display message history")
		fmt.Println("4. Filter messages by user")
		fmt.Println("5. Search messages by keyword")
		fmt.Println("6. Exit")

		fmt.Println("===============================")

		choice := readInput(
			reader,
			"Choose an option: ",
		)

		switch choice {

		case "1":

			displayUsers(users)

		case "2":

			displayUsers(users)

			senderText := readInput(
				reader,
				"Sender ID: ",
			)

			senderID, err := strconv.Atoi(
				senderText,
			)

			if err != nil {
				fmt.Println(
					"Invalid sender ID.",
				)
				continue
			}

			receiverText := readInput(
				reader,
				"Receiver ID: ",
			)

			receiverID, err := strconv.Atoi(
				receiverText,
			)

			if err != nil {
				fmt.Println(
					"Invalid receiver ID.",
				)
				continue
			}

			sender := findUser(
				users,
				senderID,
			)

			if sender == nil {
				fmt.Println(
					"Sender does not exist.",
				)
				continue
			}

			receiver := findUser(
				users,
				receiverID,
			)

			if receiver == nil {
				fmt.Println(
					"Receiver does not exist.",
				)
				continue
			}

			content := readInput(
				reader,
				"Message: ",
			)

			if content == "" {
				fmt.Println(
					"Message cannot be empty.",
				)
				continue
			}

			sendMessage(
				commands,
				*sender,
				*receiver,
				content,
				messageID,
			)

			messageID++

		case "3":

			response := make(
				chan []Message,
			)

			commands <- Command{
				Type:     GetHistory,
				Response: response,
			}

			history := <-response

			displayMessages(history)

		case "4":

			userText := readInput(
				reader,
				"User ID: ",
			)

			userID, err := strconv.Atoi(
				userText,
			)

			if err != nil {
				fmt.Println(
					"Invalid user ID.",
				)
				continue
			}

			response := make(
				chan []Message,
			)

			commands <- Command{
				Type:     FilterUser,
				UserID:   userID,
				Response: response,
			}

			results := <-response

			displayMessages(results)

		case "5":

			keyword := readInput(
				reader,
				"Keyword: ",
			)

			response := make(
				chan []Message,
			)

			commands <- Command{
				Type:     SearchKeyword,
				Keyword:  keyword,
				Response: response,
			}

			results := <-response

			displayMessages(results)

		case "6":

			commands <- Command{
				Type: Shutdown,
			}

			<-done

			fmt.Println(
				"\nChat application closed.",
			)

			return

		default:

			fmt.Println(
				"Invalid option. Choose 1-6.",
			)
		}
	}
}
