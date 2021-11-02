class SlackAction:
    def __init__(self):
        return

    def run(self, context):
        msg = context.get_input('message')
        print(msg)
        # Send message here
        context.set_output('status', 'SUCCESS')
        return
