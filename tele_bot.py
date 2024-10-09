import requests

#Send message to channel
def send_msg(bot_token,chat_id,message):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={message}"
    print(requests.get(url).json())

#Send message to test channel
def send_test_msg(message):
    url = f"https://api.telegram.org/bot5867838497:AAGIHgMV-BBdlfSCOHmOy-tevsfHRBrphyc/sendMessage?chat_id=@test_trade18&text={message}"
    requests.get(url)

#Send error message
def send_error_msg(message):
    url = f"https://api.telegram.org/bot6207519308:AAHQkvISECRzZQGD6-TDmbUrLT9hRgmecno/sendMessage?chat_id=@vis_errorBot&text={message}"
    requests.get(url)

if __name__ == "__main__":
    # send_test_msg("test")
    send_error_msg("Test sys error bot")
