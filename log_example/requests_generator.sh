#!/bin/bash

LOG_FILE="/home/ec2-user/2026-1/log_example/http_requests.log"

BASE_USERS=(
  "juan.perez@example.com"
  "ana.gomez@example.com"
  "carlos.lopez@example.com"
)

BASE_BOOKS=(
  "9780131103627"
  "9780132350884"
  "9781491950357"
  "9781098104030"
  "9780262046305"
  "9781617294433"
  "9781492078005"
  "9781789801817"
)

EVENT_TYPES=("home_view" "search" "book_list_view" "book_view" "add_to_cart" "cart_view" "checkout" "order_created")
USER_AGENTS=("Mozilla/5.0" "Chrome/124.0" "Safari/604.1" "PostmanRuntime/7.37.0")

while true
do
    # 80% clientes sintéticos, 20% clientes base
    if [ $((RANDOM % 10)) -lt 8 ]; then
        N=$(printf "%04d" $((1 + RANDOM % 400)))
        USER="synthetic.customer.${N}@example.com"
    else
        USER=${BASE_USERS[$RANDOM % ${#BASE_USERS[@]}]}
    fi

    # 70% libros sintéticos, 30% libros base
    if [ $((RANDOM % 10)) -lt 7 ]; then
        B=$(printf "%06d" $((1 + RANDOM % 120)))
        BOOK="9790000${B}"
    else
        BOOK=${BASE_BOOKS[$RANDOM % ${#BASE_BOOKS[@]}]}
    fi

    EVENT=${EVENT_TYPES[$RANDOM % ${#EVENT_TYPES[@]}]}
    UA=${USER_AGENTS[$RANDOM % ${#USER_AGENTS[@]}]}
    SESSION_ID=$(uuidgen)
    IP="172.16.2.$((10 + RANDOM % 200))"
    TIMESTAMP=$(date -u +"%d/%b/%Y:%H:%M:%S +0000")

    METHOD="GET"
    URL="/home"

    case "$EVENT" in
        home_view)
            METHOD="GET"
            URL="/home"
            ;;
        search)
            METHOD="GET"
            URL="/search?q=data+engineering"
            ;;
        book_list_view)
            METHOD="GET"
            URL="/books"
            ;;
        book_view)
            METHOD="GET"
            URL="/books/${BOOK}"
            ;;
        add_to_cart)
            METHOD="POST"
            URL="/cart/add?book_id=${BOOK}"
            ;;
        cart_view)
            METHOD="GET"
            URL="/cart"
            ;;
        checkout)
            METHOD="POST"
            URL="/checkout"
            ;;
        order_created)
            METHOD="POST"
            URL="/api/orders"
            ;;
    esac

    STATUS=200
    SIZE=$((500 + RANDOM % 15000))

    LINE="${IP} - ${USER} [${TIMESTAMP}] \"${METHOD} ${URL} HTTP/1.1\" ${STATUS} ${SIZE} \"https://bookstore.example.com\" \"${UA}\" \"session_id=${SESSION_ID} event_type=${EVENT}\""

    echo "$LINE" >> "$LOG_FILE"

    sleep 2
done