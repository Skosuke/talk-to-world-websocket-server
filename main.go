package main

import (
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
	mu      sync.Mutex
}

// 待機中のクライアントを管理するためのグローバル変数と排他制御用のミューテックス
var (
	waitingClient *Client
	waitingMutex  sync.Mutex
)

// handleWebSocket は /ws エンドポイントに対するリクエストを処理し、WebSocket 接続を確立します。
func handleWebSocket(w http.ResponseWriter, r *http.Request) {
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

	client := &Client{conn: conn}
	log.Println("新しいクライアントが接続しました")

	waitingMutex.Lock()
	if waitingClient == nil {
		// 待機中のクライアントがいなければ、このクライアントを待機状態に設定
		waitingClient = client
		waitingMutex.Unlock()

		// クライアントに待機中である旨を通知
		client.mu.Lock()
		client.conn.WriteMessage(websocket.TextMessage, []byte("Waiting for a partner..."))
		client.mu.Unlock()

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
		client.mu.Lock()
		client.conn.WriteMessage(websocket.TextMessage, []byte("Partner found! Say hi."))
		client.mu.Unlock()

		partner.mu.Lock()
		partner.conn.WriteMessage(websocket.TextMessage, []byte("Partner found! Say hi."))
		partner.mu.Unlock()

		// 新たに接続してきたクライアントは、独自のゴルーチンでメッセージ中継を開始
		go relayMessages(client)
		return
	}
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
		msgType, msg, err := client.conn.ReadMessage()
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
				client.partner.mu.Lock()
				client.partner.conn.WriteMessage(websocket.TextMessage, []byte("Your partner disconnected."))
				client.partner.conn.Close()
				client.partner.mu.Unlock()
			}
			break
		}

		// パートナーがいない場合は、クライアントに「まだ待機中」である旨を返信
		if client.partner == nil {
			client.mu.Lock()
			client.conn.WriteMessage(websocket.TextMessage, []byte("Still waiting for a partner..."))
			client.mu.Unlock()
		} else {
			// パートナーにメッセージを転送
			client.partner.mu.Lock()
			err = client.partner.conn.WriteMessage(msgType, msg)
			client.partner.mu.Unlock()
			if err != nil {
				log.Println("Write error:", err)
				break
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
