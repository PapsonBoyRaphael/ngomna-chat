class GetMessageById {
  constructor(messageRepository) {
    this.messageRepository = messageRepository;
  }

  async execute(messageId) {
    if (!messageId) throw new Error("messageId requis");
    return await this.messageRepository.findById(messageId);
  }
}

module.exports = GetMessageById;
