/**
 * Streaming response service.
 * Handles streaming Gemini API responses, managing the stop-generation button,
 * debounced message updates, large-response overflow, and post-stream finalization.
 */

import { logServiceError } from '../utils/errorHandler.js';
import {
  chatHistoryLock,
  saveStateToFile,
  shouldShowActionButtons,
  updateChatHistory,
} from '../state/botState.js';
import {
  EMBED_RESPONSE_LIMIT,
  GENERATION_ATTEMPT_TIMEOUT_MS,
  MAX_GENERATION_ATTEMPTS,
  MESSAGE_TYPING_TIMEOUT_MS,
  PLAIN_RESPONSE_LIMIT,
  SEND_RETRY_ERRORS_TO_DISCORD,
  STREAM_UPDATE_DEBOUNCE_MS,
} from '../constants.js';
import { getResponsePreference, resolveHistoryId } from './conversationContext.js';
import {
  assignFileNames,
  cleanSandboxLinks,
  extractSandboxFilenames,
  getFileExtension,
  sendCodeExecutionFiles,
} from './codeExecutionService.js';
import {
  addDeleteButton,
  addSettingsButton,
  clearMessageActionRows,
  removeStopGeneratingButton,
} from '../ui/messageActions.js';
import { applyEmbedFallback, createStatusEmbed } from '../utils/discord.js';
import {
  buildRetryErrorEmbed,
  formatGeminiErrorForConsole,
} from '../utils/errorFormatter.js';
import { toDeleteHistoryRef } from '../utils/historyRef.js';
import {
  createAttemptTimeout,
  getRetryDelayMs,
  isAbortError,
  sleep,
} from './streaming/retryController.js';
import {
  createStreamAccumulator,
  processStreamChunk,
  resetStreamAccumulatorForAttempt,
} from './streaming/chunkProcessor.js';
import { buildResponseEmbed } from './streaming/renderer.js';
import {
  createCollector,
  ensureInitialBotMessage,
} from './streaming/controls.js';
import { handleLargeOrFinalResponse } from './streaming/delivery.js';

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

function logStreamError(operation, error, metadata = {}) {
  logServiceError('StreamingService', error, { operation, ...metadata });
}

function chunkString(str, size) {
  const chunks = [];
  for (let i = 0; i < str.length; i += size) {
    chunks.push(str.substring(i, i + size));
  }
  return chunks;
}


// ---------------------------------------------------------------------------
// Conversation persistence
// ---------------------------------------------------------------------------

async function persistConversation(historyId, messageId, parts, assistantParts) {
  await chatHistoryLock.runExclusive(async () => {
    updateChatHistory(historyId, [
      { role: 'user', content: parts },
      { role: 'assistant', content: assistantParts },
    ], messageId);
    await saveStateToFile();
  });
}

// ---------------------------------------------------------------------------
// Main streaming entry point
// ---------------------------------------------------------------------------

/**
 * Streams a model response to Discord, handling retries, stop-generation,
 * large-response overflow, and post-response actions.
 *
 * @param {Object} options
 * @param {import('discord.js').Message|null} options.initialBotMessage - Existing bot message to reuse, or null.
 * @param {Object} options.chat - The Gemini chat session.
 * @param {Array} options.parts - The prompt parts to send.
 * @param {import('discord.js').Message} options.originalMessage - The user's original message.
 * @param {string[]} [options.extraMessageIds] - Optional related message IDs to bind to parent delete controls.
 */
export async function streamModelResponse({
  initialBotMessage,
  chat,
  parts,
  originalMessage,
  extraMessageIds = [],
}) {
  const historyId = resolveHistoryId(originalMessage);
  const deleteHistoryRef = toDeleteHistoryRef(historyId, originalMessage.author.id);
  const responsePreference = getResponsePreference(originalMessage);
  const maxCharacterLimit = responsePreference === 'Embedded' ? EMBED_RESPONSE_LIMIT : PLAIN_RESPONSE_LIMIT;
  let botMessage = await ensureInitialBotMessage(initialBotMessage, originalMessage);
  let botMessages = [botMessage];
  let finalized = false;
  let bufferedText = '';
  let updateTimeout = null;
  let isLargeResponse = false;
  let activeAbortController = null;

  const clearPendingUpdate = () => {
    clearTimeout(updateTimeout);
    updateTimeout = null;
  };

  const stopActiveGeneration = async () => {
    clearPendingUpdate();

    if (activeAbortController && !activeAbortController.signal.aborted) {
      activeAbortController.abort();
    }

    for (const msg of botMessages) {
      await removeStopGeneratingButton(msg);
    }
  };

  const { collector, wasStopped } = createCollector(botMessage, originalMessage, stopActiveGeneration);

  // Shared mutable accumulator updated by processStreamChunk
  const accumulator = createStreamAccumulator();

  const flushBufferedText = () => {
    if (wasStopped() || finalized) return;

    if (!bufferedText.trim()) {
      botMessage.edit(applyEmbedFallback(originalMessage.channel, {
        embeds: [createStatusEmbed({
          variant: 'muted',
          title: 'Generating Response',
          description: 'Still working on this response...',
        })],
      })).catch((error) => {
        logStreamError('flushBufferedTextPlaceholder', error, { messageId: botMessage.id });
      });
    } else if (responsePreference === 'Embedded') {
      // For embeds, always use single message (Discord embed has own limit)
      if (isLargeResponse) return;
      buildResponseEmbed(botMessage, bufferedText, originalMessage, accumulator.groundingMetadata, accumulator.urlContextMetadata).catch((error) => {
        logStreamError('flushBufferedTextEmbed', error, { messageId: botMessage.id });
      });
    } else {
      // Plain text — chunk into multiple messages if needed
      const segments = chunkString(bufferedText, maxCharacterLimit);
      for (const [index, segment] of segments.entries()) {
        if (botMessages[index]) {
          botMessages[index].edit({ content: segment, embeds: [] }).catch((error) => {
            logStreamError('flushBufferedTextPlain', error, { messageId: botMessages[index].id });
          });
        } else {
          originalMessage.reply({
            content: segment,
            embeds: [],
            allowedMentions: { users: [originalMessage.author.id], repliedUser: false },
          }).then((msg) => {
            botMessages[index] = msg;
          }).catch((error) => {
            logStreamError('flushBufferedTextPlainFollowUp', error, { userId: originalMessage.author.id });
          });
        }
      }
    }

    clearPendingUpdate();
  };

  const finalizeResponse = async (finalResponseText, responseWasLarge) => {
    const trimmedFinalResponse = finalResponseText.trim();
    const hasResponseText = trimmedFinalResponse.length > 0;
    const normalizedFinalResponse = hasResponseText
      ? trimmedFinalResponse
      : '[Empty response]';

    clearPendingUpdate();

    if (hasResponseText && responsePreference !== 'Embedded') {
      // Plain text — chunk into multiple messages if needed, delete extras
      const segments = chunkString(normalizedFinalResponse, maxCharacterLimit);
      for (const [index, segment] of segments.entries()) {
        if (botMessages[index]) {
          await botMessages[index].edit({ content: segment, embeds: [] }).catch((error) => {
            logStreamError('finalPlainEdit', error, { messageId: botMessages[index].id });
          });
        } else {
          try {
            botMessages[index] = await originalMessage.reply({
              content: segment,
              embeds: [],
              allowedMentions: { users: [originalMessage.author.id], repliedUser: false },
            });
          } catch (error) {
            logStreamError('finalPlainFollowUp', error, { userId: originalMessage.author.id });
          }
        }
      }
      // Delete any extra bot messages from streaming (if chunk count shrunk)
      for (let i = segments.length; i < botMessages.length; i++) {
        await botMessages[i].delete().catch(() => {});
      }
      botMessages = botMessages.slice(0, segments.length);
    } else if (!responseWasLarge) {
      if (responsePreference === 'Embedded') {
        await buildResponseEmbed(botMessage, normalizedFinalResponse, originalMessage, accumulator.groundingMetadata, accumulator.urlContextMetadata);
      } else {
        await botMessage.edit({ content: normalizedFinalResponse, embeds: [] }).catch((error) => {
          logStreamError('finalPlainEdit', error, { messageId: botMessage.id });
        });
      }
    }

    let filesMessage = null;
    if (accumulator.inlineDataFiles.length > 0) {
      filesMessage = await sendCodeExecutionFiles(accumulator.inlineDataFiles, originalMessage, deleteHistoryRef);
    }

    const linkedMessageIds = [
      filesMessage?.id,
      ...extraMessageIds,
    ].filter(Boolean);

    botMessage = await handleLargeOrFinalResponse(
      botMessage,
      originalMessage,
      normalizedFinalResponse,
      responseWasLarge,
      deleteHistoryRef,
      linkedMessageIds,
      botMessages,
    );

    if (hasResponseText) {
      const assistantPartsForHistory = accumulator.rawAssistantParts.length > 0
        ? accumulator.rawAssistantParts
        : [{ text: normalizedFinalResponse }];
      await persistConversation(historyId, botMessage.id, parts, assistantPartsForHistory);
    }

    finalized = true;
    activeAbortController = null;
    collector.stop('completed');
  };

  try {
    let attempts = MAX_GENERATION_ATTEMPTS;

    while (attempts > 0 && !wasStopped()) {
      let attemptTimeout = null;

      try {
        activeAbortController = new AbortController();
        attemptTimeout = createAttemptTimeout(activeAbortController, GENERATION_ATTEMPT_TIMEOUT_MS);

        const stream = await chat.sendMessageStream({
          message: parts,
          config: {
            ...(chat.config ?? {}),
            abortSignal: activeAbortController.signal,
          },
        });
        let finalResponse = '';
        isLargeResponse = false;
        resetStreamAccumulatorForAttempt(accumulator);

        for await (const chunk of stream) {
          if (wasStopped()) {
            break;
          }

          const chunkText = processStreamChunk(chunk, accumulator);

          if (chunkText) {
            finalResponse += chunkText;
            bufferedText += chunkText;
          }

          if (finalResponse.length > maxCharacterLimit) {
            if (responsePreference === 'Embedded') {
              // Embed responses overflow — show warning and send as file
              if (!isLargeResponse) {
                isLargeResponse = true;
                clearPendingUpdate();
                botMessage.edit(applyEmbedFallback(originalMessage.channel, {
                  embeds: [createStatusEmbed({
                    variant: 'warning',
                    title: 'Response Overflow',
                    description: 'This response is too long for a Discord message embed and will be delivered as an attached file.',
                  })],
                })).catch((error) => {
                  logStreamError('overflowWarningEdit', error, { messageId: botMessage.id });
                });
              }
            } else {
              // Plain text — keep streaming, chunking handles it
              if (!updateTimeout) {
                updateTimeout = setTimeout(flushBufferedText, STREAM_UPDATE_DEBOUNCE_MS);
              }
            }
          } else if (!updateTimeout) {
            updateTimeout = setTimeout(flushBufferedText, STREAM_UPDATE_DEBOUNCE_MS);
          }
        }

        // --- Post-stream processing ---

        // Determine file extensions from generated files and assign sandbox names
        const activeExtensions = accumulator.inlineDataFiles
          .map((f) => getFileExtension(f.mimeType).replace(/^\./, '').split('+')[0])
          .filter((ext) => ext && /^[a-z0-9]+$/i.test(ext));

        const sandboxFilenames = extractSandboxFilenames(finalResponse, activeExtensions);
        const actualNames = assignFileNames(accumulator.inlineDataFiles, sandboxFilenames);

        finalResponse = cleanSandboxLinks(finalResponse, actualNames);
        await finalizeResponse(finalResponse, isLargeResponse);
        attemptTimeout.clear();
        return;
      } catch (error) {
        attemptTimeout?.clear();
        const attemptTimedOut = attemptTimeout?.wasTimedOut?.() || false;
        const wasAborted = wasStopped() || activeAbortController?.signal.aborted;
        if (wasAborted && !attemptTimedOut && (isAbortError(error) || activeAbortController?.signal.aborted)) {
          const isLarge = responsePreference === 'Embedded' && bufferedText.length > maxCharacterLimit;
          await finalizeResponse(bufferedText, isLarge);
          return;
        }

        activeAbortController = null;

        attempts -= 1;
        console.error(formatGeminiErrorForConsole(error, {
          attemptNumber: MAX_GENERATION_ATTEMPTS - attempts,
          totalAttempts: MAX_GENERATION_ATTEMPTS,
          remainingAttempts: attempts,
          userId: originalMessage.author.id,
          channelId: originalMessage.channel?.id,
          historyId,
        }), error);

        if (attempts <= 0 || wasStopped()) {
          if (!wasStopped()) {
            const embed = SEND_RETRY_ERRORS_TO_DISCORD
              ? buildRetryErrorEmbed(error, { isFinal: true })
              : createStatusEmbed({
                  variant: 'error',
                  title: 'Bot Overloaded',
                  description: 'The bot is currently overloaded or unavailable. Please try again shortly.',
                });

            const errorMessage = await originalMessage.channel.send(applyEmbedFallback(originalMessage.channel, {
              content: `<@${originalMessage.author.id}>`,
              embeds: [embed],
            }));

            const linkedMessageIds = [
              ...botMessages.map(m => m.id),
              ...extraMessageIds,
            ].filter(Boolean);

            if (shouldShowActionButtons(originalMessage.guild?.id, originalMessage.author.id, originalMessage.channelId)) {
              let updatedErrorMessage = await addSettingsButton(errorMessage);
              updatedErrorMessage = await addDeleteButton(
                updatedErrorMessage,
                [updatedErrorMessage.id, ...linkedMessageIds].join(','),
                deleteHistoryRef,
              );

              for (const msg of botMessages) {
                await clearMessageActionRows(msg);
                await addSettingsButton(msg);
                await addDeleteButton(msg, [msg.id, updatedErrorMessage.id, ...extraMessageIds].join(','), deleteHistoryRef);
              }
            } else {
              for (const msg of botMessages) {
                await clearMessageActionRows(msg);
              }
            }
            finalized = true;
          }

          collector.stop();
          return;
        }

        if (SEND_RETRY_ERRORS_TO_DISCORD) {
          const retryMessage = await originalMessage.channel.send(applyEmbedFallback(originalMessage.channel, {
            content: `<@${originalMessage.author.id}>`,
            embeds: [buildRetryErrorEmbed(error, { isFinal: false })],
          }));

          setTimeout(() => {
            retryMessage.delete().catch((deleteError) => {
              logStreamError('deleteRetryMessage', deleteError, { messageId: retryMessage.id });
            });
          }, 5_000);
        }

        const attemptNumber = MAX_GENERATION_ATTEMPTS - attempts;
        const retryDelayMs = getRetryDelayMs(error, attemptNumber);

        if (retryDelayMs > 0) {
          await sleep(retryDelayMs);
        }
      }
    }
  } finally {
    if (!finalized && !wasStopped()) {
      clearPendingUpdate();
    }
    if (finalized) {
      for (const msg of botMessages) {
        await removeStopGeneratingButton(msg).catch(() => {});
      }
    }
  }
}
