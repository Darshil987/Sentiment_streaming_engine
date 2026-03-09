package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type AnalyzeRequest struct {
	Text string `json:"text"`
}

type AnalyzeResponse struct {
	Text            string   `json:"text"`
	NormalizedText  string   `json:"normalized_text"`
	Score           int      `json:"score"`
	Sentiment       string   `json:"sentiment"`
	PositiveMatches []string `json:"positive_matches"`
	NegativeMatches []string `json:"negative_matches"`
}

type Message struct {
	ID   int    `json:"id"`
	Text string `json:"text"`
}

type StreamResult struct {
	ID              int      `json:"id"`
	Text            string   `json:"text"`
	Score           int      `json:"score"`
	Sentiment       string   `json:"sentiment"`
	PositiveMatches []string `json:"positive_matches"`
	NegativeMatches []string `json:"negative_matches"`
	WorkerID        int      `json:"worker_id"`
}

type Stats struct {
	Positive int `json:"positive"`
	Negative int `json:"negative"`
	Neutral  int `json:"neutral"`
	Total    int `json:"total"`
}

type DashboardPayload struct {
	Type   string       `json:"type"`
	Result StreamResult `json:"result,omitempty"`
	Stats  Stats        `json:"stats"`
}

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	clients   = make(map[*websocket.Conn]bool)
	clientsMu sync.Mutex
	stats     Stats
	statsMu   sync.Mutex
)

func callRustWorker(text string) (AnalyzeResponse, error) {
	req := AnalyzeRequest{Text: text}

	payload, err := json.Marshal(req)
	if err != nil {
		return AnalyzeResponse{}, err
	}

	cmd := exec.Command("../rust-nlp-worker/target/debug/rust-nlp-worker.exe")
	cmd.Stdin = bytes.NewReader(payload)

	output, err := cmd.Output()
	if err != nil {
		return AnalyzeResponse{}, err
	}

	var resp AnalyzeResponse
	if err := json.Unmarshal(output, &resp); err != nil {
		return AnalyzeResponse{}, err
	}

	return resp, nil
}

func broadcast(payload DashboardPayload) {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	for client := range clients {
		err := client.WriteJSON(payload)
		if err != nil {
			client.Close()
			delete(clients, client)
		}
	}
}

func getStatsCopy() Stats {
	statsMu.Lock()
	defer statsMu.Unlock()
	return stats
}

func updateStats(sentiment string) Stats {
	statsMu.Lock()
	defer statsMu.Unlock()

	stats.Total++
	switch sentiment {
	case "Positive":
		stats.Positive++
	case "Negative":
		stats.Negative++
	default:
		stats.Neutral++
	}

	return stats
}

func worker(workerID int, jobs <-chan Message, wg *sync.WaitGroup) {
	defer wg.Done()

	for msg := range jobs {
		resp, err := callRustWorker(msg.Text)
		if err != nil {
			log.Printf("worker %d failed on message %d: %v", workerID, msg.ID, err)
			continue
		}

		currentStats := updateStats(resp.Sentiment)

		result := StreamResult{
			ID:              msg.ID,
			Text:            resp.Text,
			Score:           resp.Score,
			Sentiment:       resp.Sentiment,
			PositiveMatches: resp.PositiveMatches,
			NegativeMatches: resp.NegativeMatches,
			WorkerID:        workerID,
		}

		log.Printf("Worker %d | Message %d | %s | Score %d",
			workerID, msg.ID, resp.Sentiment, resp.Score)

		broadcast(DashboardPayload{
			Type:   "result",
			Result: result,
			Stats:  currentStats,
		})
	}
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("websocket upgrade error:", err)
		return
	}

	clientsMu.Lock()
	clients[conn] = true
	clientsMu.Unlock()

	err = conn.WriteJSON(DashboardPayload{
		Type:  "stats",
		Stats: getStatsCopy(),
	})
	if err != nil {
		conn.Close()
		clientsMu.Lock()
		delete(clients, conn)
		clientsMu.Unlock()
		return
	}

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}

	clientsMu.Lock()
	delete(clients, conn)
	clientsMu.Unlock()
	conn.Close()
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
	})
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(getStatsCopy())
}

func analyzeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req AnalyzeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	resp, err := callRustWorker(req.Text)
	if err != nil {
		http.Error(w, "failed to process sentiment", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func streamHandler(jobs chan<- Message) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		go func() {
			texts := []string{
				"This phone is amazing and fast",
				"The service was terrible and slow",
				"I love the camera quality",
				"This app has a bad issue",
				"Excellent performance and smooth experience",
				"I hate the battery life",
				"The design is nice and perfect",
				"This product is awful and broken",
				"Great value for money",
				"The experience was poor and sad",
				"Awesome display and good speed",
				"Worst update ever very bad",
			}

			for i := 1; i <= 40; i++ {
				msg := Message{
					ID:   i,
					Text: texts[rand.Intn(len(texts))],
				}
				jobs <- msg
				time.Sleep(250 * time.Millisecond)
			}
		}()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"message": "stream started",
		})
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	jobs := make(chan Message, 100)
	var wg sync.WaitGroup

	workerCount := 4
	for i := 1; i <= workerCount; i++ {
		wg.Add(1)
		go worker(i, jobs, &wg)
	}

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/stats", statsHandler)
	http.HandleFunc("/analyze", analyzeHandler)
	http.HandleFunc("/stream/start", streamHandler(jobs))
	http.HandleFunc("/ws", wsHandler)

	fmt.Println("Go API running on http://localhost:8080")
	fmt.Println("Dashboard: open dashboard/index.html in your browser")
	fmt.Println("Start stream: POST http://localhost:8080/stream/start")

	log.Fatal(http.ListenAndServe(":8080", nil))
}
