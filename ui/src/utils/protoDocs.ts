import legionConnectSource from '../gen/legion_connect.ts?raw'
import legionPbSource from '../gen/legion_pb.ts?raw'

type ProtoDocsIndex = {
  methodComments: Map<string, string>
  messageComments: Map<string, string>
  fieldCommentsByType: Map<string, Map<number, string>>
}

const cleanDocBlock = (block: string): string => {
  const lines = block
    .split('\n')
    .map((line) => line.replace(/^\s*\*\s?/, '').trimRight())

  const docs: string[] = []
  for (const line of lines) {
    if (line.startsWith('@generated')) break
    docs.push(line)
  }

  while (docs.length > 0 && docs[0] === '') docs.shift()
  while (docs.length > 0 && docs[docs.length - 1] === '') docs.pop()
  return docs.join('\n')
}

const parseMethodComments = (source: string): Map<string, string> => {
  const comments = new Map<string, string>()
  const methodRegex =
    /\/\*\*([\s\S]*?)\*\/\s*[a-zA-Z0-9_]+:\s*\{\s*name:\s*"([^"]+)"/g

  let match: RegExpExecArray | null = null
  while ((match = methodRegex.exec(source)) !== null) {
    const comment = cleanDocBlock(match[1])
    if (!comment) continue
    const methodName = match[2]
    comments.set(methodName, comment)
  }

  return comments
}

const parseMessageComments = (source: string): {
  messageComments: Map<string, string>
  fieldCommentsByType: Map<string, Map<number, string>>
} => {
  const messageComments = new Map<string, string>()
  const fieldCommentsByType = new Map<string, Map<number, string>>()

  const classRegex =
    /\/\*\*([\s\S]*?)\*\/\s*export class (\w+) extends Message<\w+> \{/g

  let classMatch: RegExpExecArray | null = null
  while ((classMatch = classRegex.exec(source)) !== null) {
    const fullComment = classMatch[1]
    const className = classMatch[2]
    const generatedTypeName =
      /@generated from message ([\w.]+)/.exec(fullComment)?.[1] ??
      `legion.${className}`
    const messageComment = cleanDocBlock(fullComment)
    if (messageComment) {
      messageComments.set(generatedTypeName, messageComment)
    }

    const classBodyStart = classMatch.index + classMatch[0].length
    const constructorStart = source.indexOf('constructor(', classBodyStart)
    if (constructorStart === -1) continue
    const classBody = source.slice(classBodyStart, constructorStart)

    const fieldCommentRegex =
      /\/\*\*([\s\S]*?)\*\/\s*[a-zA-Z_][\w$]*(?:\?)?\s*(?::[^=;]+)?(?:=[^;]+)?;/g

    let fieldMatch: RegExpExecArray | null = null
    while ((fieldMatch = fieldCommentRegex.exec(classBody)) !== null) {
      const fieldCommentBlock = fieldMatch[1]
      const fieldNoRaw = /@generated from field:\s*[^=]+=\s*(\d+);/.exec(
        fieldCommentBlock,
      )?.[1]
      if (!fieldNoRaw) continue

      const fieldNo = Number.parseInt(fieldNoRaw, 10)
      if (!Number.isFinite(fieldNo)) continue
      const docText = cleanDocBlock(fieldCommentBlock)
      if (!docText) continue

      if (!fieldCommentsByType.has(generatedTypeName)) {
        fieldCommentsByType.set(generatedTypeName, new Map<number, string>())
      }
      fieldCommentsByType.get(generatedTypeName)?.set(fieldNo, docText)
    }
  }

  return { messageComments, fieldCommentsByType }
}

const buildProtoDocsIndex = (): ProtoDocsIndex => {
  const methodComments = parseMethodComments(legionConnectSource)
  const { messageComments, fieldCommentsByType } = parseMessageComments(
    legionPbSource,
  )
  return { methodComments, messageComments, fieldCommentsByType }
}

const protoDocsIndex = buildProtoDocsIndex()

export const getMethodProtoComment = (methodName: string): string | undefined =>
  protoDocsIndex.methodComments.get(methodName)

export const getMessageProtoComment = (
  messageTypeName: string,
): string | undefined => protoDocsIndex.messageComments.get(messageTypeName)

export const getFieldProtoComment = (
  messageTypeName: string,
  fieldNumber: number,
): string | undefined =>
  protoDocsIndex.fieldCommentsByType.get(messageTypeName)?.get(fieldNumber)
