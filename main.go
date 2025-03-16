package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// pongWait は pong 応答を待つ最大時間
	pongWait = 60 * time.Second
	// pingPeriod は ping を送信する間隔。pongWait の 90% 程度とする
	pingPeriod = (pongWait * 9) / 10
)

// メッセージの構造体を定義
type Message struct {
	Type        string `json:"type"`        // "system", "message", "username_change" など
	Text        string `json:"text"`        // メッセージ本文
	Sender      string `json:"sender"`      // 送信者名
	OldUsername string `json:"oldUsername"` // 変更前のユーザー名（username_change用）
	NewUsername string `json:"newUsername"` // 変更後のユーザー名（username_change用）
}

// WebSocket のアップグレーダー設定（どのオリジンからも接続を許可）
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// Client は接続してきたクライアントの情報を保持します。
// 同一接続に対して複数のゴルーチンから書き込みが走らないよう、mu で排他制御します。
type Client struct {
	conn    *websocket.Conn
	partner *Client
	name    string // ユーザー名を保持
	mu      sync.Mutex
}

// 待機中のクライアントを管理するためのグローバル変数と排他制御用のミューテックス
var (
	waitingClient *Client
	waitingMutex  sync.Mutex
)

// システムメッセージを送信する関数
func sendSystemMessage(client *Client, text string) {
	msg := Message{
		Type:   "system",
		Text:   text,
		Sender: "System",
	}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Println("JSON Marshal error:", err)
		return
	}

	client.mu.Lock()
	defer client.mu.Unlock()

	err = client.conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		log.Println("Write error:", err)
	}
}

// handleWebSocket は /ws エンドポイントに対するリクエストを処理し、WebSocket 接続を確立します。
func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// HTTPクエリからユーザー名を取得
	username := r.URL.Query().Get("username")
	if username == "" {
		username = "Passerby" // デフォルト値
	}

	// HTTP コネクションを WebSocket にアップグレード
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}

	// pongハンドラーを設定して、接続がアイドル状態でも維持できるようにする
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	client := &Client{
		conn: conn,
		name: username,
	}
	log.Printf("新しいクライアントが接続しました: %s\n", username)

	waitingMutex.Lock()
	if waitingClient == nil {
		// 待機中のクライアントがいなければ、このクライアントを待機状態に設定
		waitingClient = client
		waitingMutex.Unlock()

		// クライアントに待機中である旨を通知
		sendSystemMessage(client, "Waiting for a partner...")

		// このままメッセージ中継（ReadMessage ループ）に入る
		relayMessages(client)
		return
	} else {
		// 既に待機中のクライアントが存在する場合、ペアリングを行う
		partner := waitingClient
		waitingClient = nil
		waitingMutex.Unlock()

		// 相互にパートナーを設定
		client.partner = partner
		partner.partner = client

		// 両クライアントにペア成立を通知
		sendSystemMessage(client, "Partner found! Say hi.")
		sendSystemMessage(partner, "Partner found! Say hi.")

		// 新たに接続してきたクライアントは、独自のゴルーチンでメッセージ中継を開始
		go relayMessages(client)
		return
	}
}

// ユーザー名変更通知メッセージを処理する関数
func handleUsernameChange(client *Client, msg Message) {
	// ユーザー名を更新
	client.name = msg.NewUsername

	// パートナーがいる場合は、ユーザー名が変更されたことを通知
	if client.partner != nil {
		notificationMsg := Message{
			Type:   "system",
			Text:   "相手のユーザー名が " + msg.OldUsername + " から " + msg.NewUsername + " に変更されました。",
			Sender: "System",
		}

		data, err := json.Marshal(notificationMsg)
		if err != nil {
			log.Println("JSON Marshal error:", err)
			return
		}

		client.partner.mu.Lock()
		err = client.partner.conn.WriteMessage(websocket.TextMessage, data)
		client.partner.mu.Unlock()

		if err != nil {
			log.Println("Write error:", err)
		}
	}

	log.Printf("ユーザー名が変更されました: %s -> %s\n", msg.OldUsername, msg.NewUsername)
}

// relayMessages は、接続されたクライアントからのメッセージをパートナーに転送します。
// なお、パートナーがいない場合は「Still waiting for a partner...」と返信し、
// 読み込みエラー時にはパートナーへの通知や待機状態の解除を行います。
func relayMessages(client *Client) {
	// 定期的に ping メッセージを送信するためのゴルーチンを開始
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(pingPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				client.mu.Lock()
				err := client.conn.WriteMessage(websocket.PingMessage, nil)
				client.mu.Unlock()
				if err != nil {
					return
				}
			case <-done:
				return
			}
		}
	}()

	defer func() {
		close(done)
		client.conn.Close()
	}()

	for {
		_, rawMsg, err := client.conn.ReadMessage()
		if err != nil {
			log.Println("Read error:", err)

			// 待機中クライアントが切断された場合、waitingClient をクリアする
			if client.partner == nil {
				waitingMutex.Lock()
				if waitingClient == client {
					waitingClient = nil
				}
				waitingMutex.Unlock()
			}

			// パートナーが存在すれば、切断通知を送り、パートナー側の接続も閉じる
			if client.partner != nil {
				sendSystemMessage(client.partner, "Your partner disconnected.")
				client.partner.conn.Close()
			}
			break
		}

		// クライアントからのメッセージをパースして処理
		var incomingMsg Message

		// JSONとして解析を試みる
		if err := json.Unmarshal(rawMsg, &incomingMsg); err != nil {
			// 従来の単純な文字列メッセージと仮定する（古いクライアント対応）
			log.Println("JSON解析エラー:", err)

			if client.partner == nil {
				sendSystemMessage(client, "Still waiting for a partner...")
			} else {
				// テキストメッセージとして処理
				textMsg := string(rawMsg)
				outgoingMsg := Message{
					Type:   "message",
					Text:   textMsg,
					Sender: client.name,
				}

				data, err := json.Marshal(outgoingMsg)
				if err != nil {
					log.Println("JSON Marshal error:", err)
					continue
				}

				client.partner.mu.Lock()
				err = client.partner.conn.WriteMessage(websocket.TextMessage, data)
				client.partner.mu.Unlock()

				if err != nil {
					log.Println("Write error:", err)
					break
				}
			}
		} else {
			// 正常にJSONとして解析できた場合
			switch incomingMsg.Type {
			case "message":
				// 通常のチャットメッセージ
				if client.partner == nil {
					sendSystemMessage(client, "Still waiting for a partner...")
				} else {
					// メッセージを送信者名を含めて転送
					outgoingMsg := Message{
						Type:   "message",
						Text:   incomingMsg.Text,
						Sender: client.name, // クライアントの現在の名前を使用
					}

					data, err := json.Marshal(outgoingMsg)
					if err != nil {
						log.Println("JSON Marshal error:", err)
						continue
					}

					client.partner.mu.Lock()
					err = client.partner.conn.WriteMessage(websocket.TextMessage, data)
					client.partner.mu.Unlock()

					if err != nil {
						log.Println("Write error:", err)
						break
					}
				}
			case "username_change":
				// ユーザー名変更メッセージ
				handleUsernameChange(client, incomingMsg)
			}
		}
	}
}

func main() {
	// /ws エンドポイントに対してハンドラを設定
	http.HandleFunc("/ws", handleWebSocket)

	log.Println("WebSocket サーバーを :8080 で起動します")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}
