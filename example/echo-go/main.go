// telegateway example bot (Go, stdlib only).
// Listens on 127.0.0.1:8082 and echoes every message back via sendMessage.
// Without TELEGRAM_BOT_TOKEN it logs what it would send and answers 200.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

type update struct {
	UpdateID int `json:"update_id"`
	Message  *struct {
		Text string `json:"text"`
		Chat struct {
			ID int64 `json:"id"`
		} `json:"chat"`
	} `json:"message"`
}

func sendMessage(token string, chatID int64, text string) error {
	body, _ := json.Marshal(map[string]any{"chat_id": chatID, "text": text})
	req, _ := http.NewRequest(
		"POST",
		"https://api.telegram.org/bot"+token+"/sendMessage",
		bytes.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("telegram answered %s", resp.Status)
	}
	return nil
}

func main() {
	token := os.Getenv("TELEGRAM_BOT_TOKEN")
	http.HandleFunc("/webhook", func(w http.ResponseWriter, r *http.Request) {
		var u update
		if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		if u.Message != nil && u.Message.Text != "" {
			log.Printf("echo-go: update %d from chat %d", u.UpdateID, u.Message.Chat.ID)
			if token == "" {
				log.Printf("[dry-run] would send to %d: %s", u.Message.Chat.ID, u.Message.Text)
			} else if err := sendMessage(token, u.Message.Chat.ID, u.Message.Text); err != nil {
				log.Printf("sendMessage failed: %v", err)
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	log.Println("echo-go listening on 127.0.0.1:8082")
	log.Fatal(http.ListenAndServe("127.0.0.1:8082", nil))
}
